import subprocess
import time
import sys
import os
import uuid
import argparse
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Permite importar modulos de omie_estoque executando pela raiz do repo.
BASE_DIR = Path(__file__).parent
ESTOQUE_DIR = BASE_DIR / "omie_estoque"
sys.path.insert(0, str(ESTOQUE_DIR))

from logger_config import obter_logger
from supabase_events import SupabaseEventLogger, get_supabase_config_from_env

# Configurar logging
logger = obter_logger("main_pipeline")

# Identidade do "projeto" para logs no Supabase (separar reposição vs vendas)
DEFAULT_SUPABASE_APP = "p79-maqtools"
DEFAULT_SUPABASE_PROJECT_KEY = "p79-maqtools-reposicao"

# =============================================================================
# PIPELINE ETL OMIE - ARQUITETURA MEDALHÃO
# =============================================================================

scripts_bronze = [
    "nbronze_produto_fornecedor.py",
    "nbronze_nfconsultar.py",
    "nbronze_clientes_fornecedores.py",
    "nbronze_produtos.py",
    "nbronze_servicos.py",
    "nbronze_pedido.py",
    "nbronze_etapas_faturamento.py",
    "nbronze_produto_estoque.py",
    "nbronze_pedido_compra.py",
    "nbronze_pedido_compra_cancelado.py",
    "nbronze_pedido_compra_encerrado.py",
]

scripts_silver = [
    "nsilver_nfconsultar_stg.py",
    "nsilver_nfconsultar.py",
    "nsilver_produto_fornecedor.py",
    "nsilver_cliente.py",
    "nsilver_produto.py",
    "nsilver_servicos.py",
    "nsilver_pedido.py",
    "nsilver_nfconsultar_stg.py",
    "nsilver_pedido_compra_stg.py",
    "nsilver_pedido_compra_cancelado.py",
    "nsilver_pedido_compra_encerrado.py",
    "nsilver_pedido_compra.py",
    "nsilver_etapas_faturamento.py",
    "nsilver_det_produtos_pedido.py",
    "nsilver_pedidos_repetidos.py",
]

scripts_gold = [
    "ngold_servicos_produtos.py",
    "ngold_pedido.py",
    "ngold_pedido_compra.py",
    "ngold_servicos_produtos_fornecedor.py",
    "ngold_produto_fornecedor_full.py",
    "ngold_produto_fornecedor_validacao.py",
    "atualiza_dataset.py",
]


def formatar_tempo(segundos):
    if segundos < 60:
        return f"{int(segundos)} segundos"
    if segundos < 3600:
        minutos = int(segundos // 60)
        segs = int(segundos % 60)
        return f"{minutos} minutos e {segs} segundos"
    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    return f"{horas} horas e {minutos} minutos"


def _supabase_env() -> str:
    return (os.environ.get("NODE_ENV") or os.environ.get("ENV") or "prod").strip()


def _supabase_app() -> str:
    return (os.environ.get("SUPABASE_APP") or DEFAULT_SUPABASE_APP).strip()


def _supabase_project_key() -> str:
    return (os.environ.get("SUPABASE_PROJECT_KEY") or DEFAULT_SUPABASE_PROJECT_KEY).strip()


def _script_table_name(camada: str, script: str) -> str:
    # Mantém o padrão do p110-cordeiro (campo table_name) mas com scripts.
    return f"{camada}_{script}"


def _strip_env_path(raw: str) -> str:
    s = (raw or "").strip().strip('"').strip("'")
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff")
    return s


_GCS_CRED_WARNED = False


def _ensure_gcs_credentials_env() -> None:
    """
    Garante GCS_CREDENTIALS_JSON_PATH absoluto no ambiente antes dos subprocessos.
    Os scripts rodam com cwd=omie_estoque; sem isso, caminhos relativos do .env podem
    não bater com a resolução feita só no filho.
    """
    load_dotenv(BASE_DIR / ".env")

    def _set_if_file(path: Path) -> bool:
        try:
            r = path.expanduser().resolve()
        except OSError:
            return False
        if r.is_file():
            os.environ["GCS_CREDENTIALS_JSON_PATH"] = str(r)
            return True
        return False

    key_path = _strip_env_path(os.environ.get("GCS_CREDENTIALS_JSON_PATH", ""))
    if key_path:
        p = Path(key_path)
        if p.is_absolute():
            if _set_if_file(p):
                return
        else:
            for base in (BASE_DIR, ESTOQUE_DIR):
                if _set_if_file(base / key_path):
                    return
                if _set_if_file(base / p.name):
                    return

    mtk = _strip_env_path(os.environ.get("MACHTOOLS_JSON_PATH", ""))
    if mtk:
        p = Path(mtk)
        if p.is_absolute():
            if _set_if_file(p):
                return
        else:
            for base in (BASE_DIR, ESTOQUE_DIR):
                if _set_if_file(base / mtk):
                    return
                if _set_if_file(base / p.name):
                    return

    gac = _strip_env_path(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))
    if not gac:
        return
    name = Path(gac).name
    for cand in (
        Path(gac),
        BASE_DIR / gac,
        ESTOQUE_DIR / gac,
        BASE_DIR / name,
        ESTOQUE_DIR / name,
    ):
        if _set_if_file(cand):
            return
    if os.name == "nt":
        up = _strip_env_path(os.environ.get("USERPROFILE", ""))
        if up:
            if _set_if_file(Path(up) / gac):
                return
            if _set_if_file(Path(up) / name):
                return

    global _GCS_CRED_WARNED
    k = os.environ.get("GCS_CREDENTIALS_JSON_PATH", "").strip()
    if (not k or not Path(k).is_file()) and not _GCS_CRED_WARNED:
        _GCS_CRED_WARNED = True
        logger.warning(
            "Nenhum JSON de service account GCP encontrado a partir do .env "
            "(procurei na raiz do repo, omie_estoque e pasta do usuario Windows). "
            "Copie o arquivo da GCP para essa pasta ou defina GCS_CREDENTIALS_JSON_PATH com caminho absoluto."
        )


def _resolver_camada_por_script(script: str) -> str | None:
    if script in scripts_bronze:
        return "bronze"
    if script in scripts_silver:
        return "silver"
    if script in scripts_gold:
        return "gold"
    return None


def run_single_script(script: str) -> None:
    load_dotenv(BASE_DIR / ".env")
    inicio = time.time()
    etapas_info: list[dict] = []
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    camada = _resolver_camada_por_script(script)
    if not camada:
        raise ValueError(
            f"Script '{script}' não encontrado nas listas bronze/silver/gold do main_omie_estoque.py"
        )

    ev = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_supabase_project_key(),
        )
        ev.emit(
            "run_start",
            extra={
                "stages": [camada],
                "entrypoint": "main_omie_estoque.py",
                "camadas": [camada],
                "tabelas_detalhadas": [_script_table_name(camada, script)],
                "modo": "single_script",
            },
        )

    try:
        logger.info(f"=== EXECUTANDO APENAS 1 SCRIPT ({camada}) ===")
        print(f"\n=== EXECUTANDO APENAS 1 SCRIPT ({camada}) ===")
        run_script(script, etapas_info, ev=ev, camada=camada)

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.info("Script executado com sucesso!")
        print("\n[OK] Script executado com sucesso!")

        diagnostico = {
            "Modo": "Single Script",
            "Camada": camada,
            "Script": script,
            "Tempo de Execucao": tempo_formatado,
            "Status": "Concluido com sucesso",
        }
    except Exception as e:
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.error(f"Erro na execucao do script {script}: {e}")
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit("run_end", duration_seconds=dur_total, extra={"stages": [camada]})
            ev.flush()
            ev.close()


def run_script(script, etapas_info=None, *, ev: SupabaseEventLogger | None = None, camada: str):
    inicio_script = datetime.now()
    t0 = time.perf_counter()
    logger.info(f"Executando: {script}")
    print(f"\n[EXECUTANDO] {script}")

    _ensure_gcs_credentials_env()

    table_name = _script_table_name(camada, script)
    if ev:
        ev.emit("table_start", table_name=table_name, extra={"camada": camada, "script": script})

    cmd = [sys.executable]
    if sys.version_info >= (3, 7):
        cmd.extend(["-X", "utf8"])
    cmd.append(script)
    result = subprocess.run(
        cmd,
        cwd=str(ESTOQUE_DIR),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    print(result.stdout)
    if result.returncode != 0:
        duration = time.perf_counter() - t0
        logger.error(f"Erro ao executar {script}:\n{result.stderr}")
        print(f"[ERRO] Erro ao executar {script}:\n{result.stderr}")
        if ev:
            ev.emit(
                "table_end",
                level="ERROR",
                table_name=table_name,
                duration_seconds=duration,
                error_type=f"subprocess_returncode_{result.returncode}",
                error_message=(result.stderr or "").strip()[:4000],
                extra={
                    "status": "fail",
                    "camada": camada,
                    "script": script,
                },
            )
        raise Exception(f"Erro ao executar {script}:\n{result.stderr}")
    logger.info(f"✓ Script {script} executado com sucesso")

    fim_script = datetime.now()
    duration = time.perf_counter() - t0
    if ev:
        ev.emit(
            "table_end",
            table_name=table_name,
            duration_seconds=duration,
            extra={
                "status": "ok",
                "camada": camada,
                "script": script,
            },
        )
    if etapas_info is not None:
        etapas_info.append(
            {
                "nome": script,
                "inicio": inicio_script.strftime("%d/%m/%Y %H:%M:%S"),
                "fim": fim_script.strftime("%d/%m/%Y %H:%M:%S"),
                "duracao": formatar_tempo((fim_script - inicio_script).total_seconds()),
            }
        )


def run_silver_only():
    load_dotenv(BASE_DIR / ".env")
    inicio = time.time()
    etapas_info = []
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    ev = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_supabase_project_key(),
        )
        ev.emit(
            "run_start",
            extra={
                "stages": ["silver"],
                "entrypoint": "main_omie_estoque.py",
                "camadas": ["silver"],
                "tabelas_detalhadas": [_script_table_name("silver", s) for s in scripts_silver],
            },
        )

    try:
        logger.info("=== EXECUTANDO APENAS CAMADA SILVER ===")
        print("\n=== EXECUTANDO APENAS CAMADA SILVER ===")

        for script in scripts_silver:
            run_script(script, etapas_info, ev=ev, camada="silver")

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        logger.info("Camada Silver executada com sucesso!")
        print("\n[OK] Camada Silver executada com sucesso!")
        print("[OK] Processamento da camada Silver finalizado!")

        diagnostico = {
            "Camada Executada": "Silver",
            "Scripts Executados": f"{len(scripts_silver)} scripts",
            "Tempo de Execucao": tempo_formatado,
            "Status": "Concluido com sucesso",
        }

    except Exception as e:
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.error(f"Erro na execucao da camada Silver: {e}")
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit("run_end", duration_seconds=dur_total, extra={"stages": ["silver"]})
            ev.flush()
            ev.close()


def run_gold_only():
    load_dotenv(BASE_DIR / ".env")
    inicio = time.time()
    etapas_info = []
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    ev = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_supabase_project_key(),
        )
        ev.emit(
            "run_start",
            extra={
                "stages": ["gold"],
                "entrypoint": "main_omie_estoque.py",
                "camadas": ["gold"],
                "tabelas_detalhadas": [_script_table_name("gold", s) for s in scripts_gold],
            },
        )

    try:
        logger.info("=== EXECUTANDO APENAS CAMADA GOLD ===")
        print("\n=== EXECUTANDO APENAS CAMADA GOLD ===")

        for script in scripts_gold:
            run_script(script, etapas_info, ev=ev, camada="gold")

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        logger.info("Camada Gold executada com sucesso!")
        print("\n[OK] Camada Gold executada com sucesso!")
        print("[OK] Processamento da camada Gold finalizado!")

        diagnostico = {
            "Camada Executada": "Gold",
            "Scripts Executados": f"{len(scripts_gold)} scripts",
            "Tempo de Execucao": tempo_formatado,
            "Status": "Concluido com sucesso",
        }

    except Exception as e:
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.error(f"Erro na execucao da camada Gold: {e}")
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit("run_end", duration_seconds=dur_total, extra={"stages": ["gold"]})
            ev.flush()
            ev.close()


def run_silver_gold():
    """Silver seguido de Gold (sem Bronze), para validar essas camadas."""
    load_dotenv(BASE_DIR / ".env")
    inicio = time.time()
    etapas_info = []
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    ev = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_supabase_project_key(),
        )
        tabelas = [_script_table_name("silver", s) for s in scripts_silver] + [
            _script_table_name("gold", s) for s in scripts_gold
        ]
        ev.emit(
            "run_start",
            extra={
                "stages": ["silver", "gold"],
                "entrypoint": "main_omie_estoque.py",
                "camadas": ["silver", "gold"],
                "tabelas_detalhadas": tabelas,
            },
        )

    try:
        logger.info("=== EXECUTANDO CAMADAS SILVER E GOLD ===")
        print("\n=== EXECUTANDO CAMADAS SILVER E GOLD ===")

        logger.info("=== CAMADA SILVER ===")
        print("\n=== CAMADA SILVER ===")
        for script in scripts_silver:
            run_script(script, etapas_info, ev=ev, camada="silver")

        logger.info("=== CAMADA GOLD ===")
        print("\n=== CAMADA GOLD ===")
        for script in scripts_gold:
            run_script(script, etapas_info, ev=ev, camada="gold")

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.info("Camadas Silver e Gold executadas com sucesso!")
        print("\n[OK] Camadas Silver e Gold executadas com sucesso!")

        diagnostico = {
            "Camadas": "Silver + Gold",
            "Scripts Silver": f"{len(scripts_silver)}",
            "Scripts Gold": f"{len(scripts_gold)}",
            "Tempo de Execucao": tempo_formatado,
            "Status": "Concluido com sucesso",
        }

    except Exception as e:
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.error(f"Erro na execucao Silver+Gold: {e}")
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit(
                "run_end",
                duration_seconds=dur_total,
                extra={"stages": ["silver", "gold"]},
            )
            ev.flush()
            ev.close()


def main():
    load_dotenv(BASE_DIR / ".env")
    inicio = time.time()
    etapas_info = []
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    ev = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_supabase_project_key(),
        )
        ev.emit(
            "run_start",
            extra={
                "stages": ["bronze", "silver", "gold"],
                "entrypoint": "main_omie_estoque.py",
                "camadas": ["bronze", "silver", "gold"],
                "tabelas_detalhadas": (
                    [_script_table_name("bronze", s) for s in scripts_bronze]
                    + [_script_table_name("silver", s) for s in scripts_silver]
                    + [_script_table_name("gold", s) for s in scripts_gold]
                ),
            },
        )

    try:
        logger.info("=== INICIANDO PIPELINE ETL COMPLETO ===")

        logger.info("=== CAMADA BRONZE ===")
        print("\n=== CAMADA BRONZE ===")
        for script in scripts_bronze:
            run_script(script, etapas_info, ev=ev, camada="bronze")

        logger.info("=== CAMADA SILVER ===")
        print("\n=== CAMADA SILVER ===")
        for script in scripts_silver:
            run_script(script, etapas_info, ev=ev, camada="silver")

        logger.info("=== CAMADA GOLD ===")
        print("\n=== CAMADA GOLD ===")
        for script in scripts_gold:
            run_script(script, etapas_info, ev=ev, camada="gold")

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        logger.info("Pipeline ETL completo executado com sucesso!")
        logger.info("Todas as 3 camadas (Bronze > Silver > Gold) foram processadas!")
        print("\n[OK] Pipeline ETL completo executado com sucesso!")
        print("[OK] Todas as 3 camadas (Bronze -> Silver -> Gold) foram processadas!")

        total_scripts = len(scripts_bronze) + len(scripts_silver) + len(scripts_gold)
        diagnostico = {
            "Pipeline": "Completo (Bronze > Silver > Gold)",
            "Scripts Bronze": f"{len(scripts_bronze)} scripts",
            "Scripts Silver": f"{len(scripts_silver)} scripts",
            "Scripts Gold": f"{len(scripts_gold)} scripts",
            "Total de Scripts": f"{total_scripts} scripts",
            "Tempo Total": tempo_formatado,
            "Status": "Concluido com sucesso",
        }

    except Exception as e:
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.error(f"Erro na execucao do pipeline: {e}")
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit(
                "run_end",
                duration_seconds=dur_total,
                extra={"stages": ["bronze", "silver", "gold"]},
            )
            ev.flush()
            ev.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL omie_estoque (Bronze → Silver → Gold).")
    modo = parser.add_mutually_exclusive_group()
    modo.add_argument("--silver-only", action="store_true", help="Executa apenas a camada Silver.")
    modo.add_argument("--gold-only", action="store_true", help="Executa apenas a camada Gold.")
    modo.add_argument(
        "--silver-gold",
        action="store_true",
        help="Executa Silver e Gold em sequência (sem Bronze).",
    )
    parser.add_argument(
        "--script",
        type=str,
        default=None,
        help="Executa apenas um script específico (ex.: nbronze_produto_fornecedor.py).",
    )
    args = parser.parse_args()

    if args.script:
        run_single_script(args.script.strip())
    elif args.silver_only:
        run_silver_only()
    elif args.gold_only:
        run_gold_only()
    elif args.silver_gold:
        run_silver_gold()
    else:
        main()

