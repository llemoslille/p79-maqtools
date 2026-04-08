"""
=============================================================================
PIPELINE ETL - PROJETO OMIE VENDAS
=============================================================================

Entrypoint dedicado para executar SOMENTE o projeto `omie_vendas`, com:
- Orquestração por camadas (omie/raw/silver/gold/modelo_semantico)
- Execução dos scripts via subprocess com cwd correto
- Paralelismo opcional nas camadas compatíveis
- Logs de eventos no Supabase (quando variáveis de ambiente estiverem presentes)
=============================================================================
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Iterable

from dotenv import load_dotenv

from supabase_events import SupabaseEventLogger, get_supabase_config_from_env


BASE_DIR = Path(__file__).parent
VENDAS_DIR = BASE_DIR / "omie_vendas"

# Saída ordenada quando --paralelo (vários subprocessos imprimem ao mesmo tempo)
_print_lock = Lock()

# Identidade do "projeto" para logs no Supabase (separar reposição vs vendas)
DEFAULT_SUPABASE_APP = "p79-maqtools"
DEFAULT_SUPABASE_PROJECT_KEY = "p79-maqtools-vendas"


SCRIPTS_OMIE = [
    "Omie.py",
    "Omie_crm.py",
    "Omie_produtos.py",
    "Omie_produtos_fornecedor.py",
    "Omie_vendedores.py",
]

SCRIPTS_RAW = [
    "raw_clientes.py",
    "raw_crm_oportunidades.py",
    "raw_etapas_faturamento.py",
    "raw_pedido_vendas.py",
    "raw_produto_fornecedor.py",
    "raw_servico_nfse.py",
    "raw_servico_os.py",
]

SCRIPTS_SILVER = [
    "silver_clientes.py",
    "silver_servico_nfse.py",
    "silver_servico_os.py",
]

SCRIPTS_GOLD = [
    "gold_servico_os.py",
    "gold_fornecedor_produto.py",
]

SCRIPTS_MODELO_SEMANTICO = [
    "atualiza_dataset.py",
]

CAMADAS: dict[str, list[str]] = {
    "raw": SCRIPTS_RAW,
    "omie": SCRIPTS_OMIE,
    "silver": SCRIPTS_SILVER,
    "gold": SCRIPTS_GOLD,
    "modelo_semantico": SCRIPTS_MODELO_SEMANTICO,
}


def _camadas_permitidas() -> list[str]:
    return list(CAMADAS.keys())


def _supabase_env() -> str:
    return (os.environ.get("NODE_ENV") or os.environ.get("ENV") or "prod").strip()


def _supabase_app() -> str:
    return (os.environ.get("SUPABASE_APP") or DEFAULT_SUPABASE_APP).strip()


def _project_key() -> str:
    return (os.environ.get("SUPABASE_PROJECT_KEY") or DEFAULT_SUPABASE_PROJECT_KEY).strip()


def _table_name(camada: str, script: str) -> str:
    return f"{camada}_{script}"


def _emit_run_start(ev: SupabaseEventLogger, camadas: Iterable[str]) -> None:
    camadas_list = list(camadas)
    tabelas = []
    for c in camadas_list:
        for s in CAMADAS.get(c, []):
            tabelas.append(_table_name(c, s))
    ev.emit(
        "run_start",
        extra={
            "stages": camadas_list,
            "entrypoint": "main_omie_vendas.py",
            "camadas": camadas_list,
            "tabelas_detalhadas": tabelas,
            "modo": "pipeline",
        },
    )


def _resolver_camada_por_script(script: str) -> str | None:
    for camada, scripts in CAMADAS.items():
        if script in scripts:
            return camada
    return None


def formatar_tempo(segundos: float) -> str:
    if segundos < 60:
        return f"{int(segundos)} segundos"
    if segundos < 3600:
        minutos = int(segundos // 60)
        segs = int(segundos % 60)
        return f"{minutos} minutos e {segs} segundos"
    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    return f"{horas} horas e {minutos} minutos"


def _run_script_via_subprocess(
    script_path: Path, working_dir: Path, script_id: str = ""
) -> tuple[bool, str, float, str | None]:
    """Executa um script Python em subprocesso (cwd + UTF-8 + PYTHONPATH na raiz do repo)."""
    inicio = time.time()
    script_name = script_path.name

    if not script_path.exists():
        erro = f"Script nao encontrado: {script_path}"
        with _print_lock:
            print(f"\n[ERRO] [{script_id}] {erro}")
        return (False, script_name, 0.0, erro)

    with _print_lock:
        print(f"\n{'=' * 80}")
        print(f"[{script_id}] Executando: {script_name}")
        print(f"Diretorio: {working_dir}")
        print(f"{'=' * 80}")

    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        root = str(BASE_DIR)
        env["PYTHONPATH"] = root + os.pathsep + (env.get("PYTHONPATH") or "")

        if sys.platform == "win32":
            try:
                if hasattr(sys.stdout, "reconfigure"):
                    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
                    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

        # Mesmo interpretador deste processo (evita "abrir com..." no Agendador de Tarefas)
        python_cmd = [sys.executable]
        if sys.version_info >= (3, 7):
            python_cmd.extend(["-X", "utf8"])
        python_cmd.append(str(script_path))

        result = subprocess.run(
            python_cmd,
            cwd=str(working_dir),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            timeout=None,
        )

        tempo = time.time() - inicio

        with _print_lock:
            if result.stdout:
                print(f"\n[{script_id}] STDOUT - {script_name}:")
                print(result.stdout)
            if result.stderr:
                print(f"\n[{script_id}] STDERR - {script_name}:")
                print(result.stderr)

        if result.returncode != 0:
            erro_msg = f"Erro ao executar {script_name} (codigo: {result.returncode})"
            if result.stdout:
                erro_msg += f"\nSTDOUT:\n{result.stdout}"
            if result.stderr:
                erro_msg += f"\nSTDERR:\n{result.stderr}"
            with _print_lock:
                print(f"\n[ERRO] [{script_id}] {erro_msg}")
            return (False, script_name, tempo, erro_msg)

        with _print_lock:
            print(
                f"\n[OK] [{script_id}] {script_name} concluido! ({formatar_tempo(tempo)})"
            )
        return (True, script_name, tempo, None)

    except subprocess.TimeoutExpired:
        tempo = time.time() - inicio
        erro_msg = f"Timeout ao executar {script_name}"
        with _print_lock:
            print(f"\n[ERRO] [{script_id}] {erro_msg}")
        return (False, script_name, tempo, erro_msg)
    except Exception as e:
        tempo = time.time() - inicio
        erro = f"Falha ao executar {script_name}: {str(e)}"
        with _print_lock:
            print(f"\n[ERRO] [{script_id}] {erro}")
        return (False, script_name, tempo, erro)


def _executar_script(
    *,
    camada: str,
    script: str,
    idx: int,
    ev: SupabaseEventLogger | None,
) -> None:
    script_path = VENDAS_DIR / script
    script_id = f"ven-{camada[:2]}-{idx}"
    t0 = time.perf_counter()

    table_name = _table_name(camada, script)
    if ev:
        ev.emit("table_start", table_name=table_name, extra={"camada": camada, "script": script})

    sucesso, nome, _tempo, erro = _run_script_via_subprocess(
        script_path, VENDAS_DIR, script_id
    )
    duration = time.perf_counter() - t0

    if not sucesso:
        if ev:
            ev.emit(
                "table_end",
                level="ERROR",
                table_name=table_name,
                duration_seconds=duration,
                error_type="subprocess_failed",
                error_message=(erro or "")[:4000],
                extra={"status": "fail", "camada": camada, "script": script},
            )
        raise RuntimeError(f"Script {nome} falhou: {erro}")

    if ev:
        ev.emit(
            "table_end",
            table_name=table_name,
            duration_seconds=duration,
            extra={"status": "ok", "camada": camada, "script": script},
        )


def run_single_script(*, script: str) -> None:
    load_dotenv(BASE_DIR / ".env")

    script = script.strip()
    camada = _resolver_camada_por_script(script)
    if not camada:
        raise ValueError(
            f"Script '{script}' não encontrado nas listas do projeto omie_vendas."
        )

    inicio = time.time()
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    ev: SupabaseEventLogger | None = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_project_key(),
        )
        ev.emit(
            "run_start",
            extra={
                "stages": [camada],
                "entrypoint": "main_omie_vendas.py",
                "camadas": [camada],
                "tabelas_detalhadas": [_table_name(camada, script)],
                "modo": "single_script",
            },
        )

    try:
        print("\n" + "=" * 80)
        print("PIPELINE ETL - PROJETO OMIE VENDAS (SINGLE SCRIPT)")
        print("=" * 80)
        print(f"SCRIPT: {script}")
        print(f"CAMADA: {camada}")
        print("=" * 80)

        _executar_script(camada=camada, script=script, idx=1, ev=ev)

        tempo_total = time.time() - inicio
        print("\n" + "=" * 80)
        print("[OK] SCRIPT FINALIZADO COM SUCESSO!")
        print("=" * 80)
        print(f"TEMPO TOTAL: {formatar_tempo(tempo_total)}")
        print("=" * 80)
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit("run_end", duration_seconds=dur_total, extra={"stages": [camada]})
            ev.flush()
            ev.close()


def main(*, camada_especifica: str | None = None, paralelo: bool = False, max_workers: int = 8) -> None:
    load_dotenv(BASE_DIR / ".env")

    inicio = time.time()
    start_global = time.perf_counter()
    run_id = uuid.uuid4()

    camadas_execucao = [c for c in _camadas_permitidas() if (not camada_especifica or c == camada_especifica)]

    print("\n" + "=" * 80)
    print("PIPELINE ETL - PROJETO OMIE VENDAS")
    print("=" * 80)
    print(f"INICIO: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"CAMADAS: {', '.join(camadas_execucao) if camadas_execucao else '(nenhuma)'}")
    print(f"Modo: {'PARALELO' if paralelo else 'SEQUENCIAL'}")
    if paralelo:
        print(f"Workers: {max_workers}")
    print("=" * 80)

    ev: SupabaseEventLogger | None = None
    supa_cfg = get_supabase_config_from_env()
    if supa_cfg:
        ev = SupabaseEventLogger(
            supa_cfg,
            run_id=run_id,
            app=_supabase_app(),
            env=_supabase_env(),
            project_key=_project_key(),
        )
        _emit_run_start(ev, camadas_execucao)

    try:
        for camada in camadas_execucao:
            scripts = CAMADAS.get(camada, [])
            if not scripts:
                continue

            # Paralelismo somente nas camadas onde isso não costuma quebrar dependências.
            pode_paralelo = paralelo and camada in ["omie", "raw", "silver"] and len(scripts) > 1

            if pode_paralelo:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=min(max_workers, len(scripts))) as ex:
                    futures = []
                    for i, script in enumerate(scripts, 1):
                        futures.append(ex.submit(_executar_script, camada=camada, script=script, idx=i, ev=ev))
                    for f in as_completed(futures):
                        f.result()
            else:
                for i, script in enumerate(scripts, 1):
                    _executar_script(camada=camada, script=script, idx=i, ev=ev)

        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)
        print("\n" + "=" * 80)
        print("PIPELINE FINALIZADO COM SUCESSO!")
        print("=" * 80)
        print(f"TEMPO TOTAL: {tempo_formatado}")
        print("=" * 80)

    except Exception:
        # Mantém o stacktrace no log principal do runner (stderr/console).
        raise
    finally:
        if ev:
            dur_total = time.perf_counter() - start_global
            ev.emit("run_end", duration_seconds=dur_total, extra={"stages": camadas_execucao})
            ev.flush()
            ev.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL omie_vendas (com Supabase events).")
    parser.add_argument(
        "--camada",
        type=str,
        choices=_camadas_permitidas(),
        help="Executar apenas uma camada especifica",
    )
    parser.add_argument("--listar", action="store_true", help="Listar camadas e total de scripts.")
    parser.add_argument(
        "--script",
        type=str,
        default=None,
        help="Executa apenas um script específico (ex.: raw_clientes.py).",
    )
    parser.add_argument("--paralelo", action="store_true", help="Habilitar execucao paralela (padrao: sequencial).")
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Numero maximo de workers para execucao paralela (padrao: 8, requer --paralelo).",
    )
    args = parser.parse_args()

    if args.listar:
        print("\n" + "=" * 80)
        print("PROJETO: omie_vendas")
        print("=" * 80)
        for camada in _camadas_permitidas():
            print(f"  - {camada}: {len(CAMADAS[camada])} scripts")
        print("=" * 80 + "\n")
        sys.exit(0)

    try:
        if args.script:
            run_single_script(script=args.script)
        else:
            main(camada_especifica=args.camada, paralelo=args.paralelo, max_workers=args.workers)
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n\n[AVISO] Execucao interrompida pelo usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[ERRO] Execucao finalizada com erro: {e}")
        sys.exit(1)

