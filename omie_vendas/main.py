"""
=============================================================================
PIPELINE ETL MACHTOOLS - PROJETO VENDAS
=============================================================================

Pipeline completo de ETL seguindo arquitetura medalhão:
- OMIE: Coleta de dados da API Omie
- RAW: Dados brutos sem transformação
- SILVER: Dados limpos e normalizados
- GOLD: Dados consolidados e prontos para análise
- MODELO SEMÂNTICO: Atualização do dataset no BigQuery

Autor: Equipe de Dados
Data: 2025
=============================================================================
"""

import subprocess
import time
import traceback
import os
from datetime import datetime
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("main_pipeline")

# =============================================================================
# DEFINIÇÃO DOS SCRIPTS POR CAMADA
# =============================================================================

# CAMADA OMIE - Coleta de dados da API
scripts_omie = [
    'Omie.py',
    'Omie_crm.py',
    'Omie_produtos.py',
    'Omie_produtos_fornecedor.py',
    'Omie_vendedores.py',
]

# CAMADA RAW - Dados brutos
scripts_raw = [
    'raw_crm_oportunidades.py',
    'raw_etapas_faturamento.py',
    'raw_pedido_vendas.py',
    'raw_produto_fornecedor.py',
    'raw_servico_nfse.py',
    'raw_servico_os.py',
]

# CAMADA SILVER - Dados limpos e normalizados
scripts_silver = [
    'silver_servico_nfse.py',
    'silver_servico_os.py',
]

# CAMADA GOLD - Dados consolidados
scripts_gold = [
    'gold_servico_os.py',
    'gold_fornecedor_produto.py',
]

# MODELO SEMÂNTICO - Atualização do dataset
scripts_modelo_semantico = [
    'atualiza_dataset.py',
]


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def formatar_tempo(segundos):
    """Formata o tempo em segundos para formato legível"""
    if segundos < 60:
        return f"{int(segundos)} segundos"
    elif segundos < 3600:
        minutos = int(segundos // 60)
        segs = int(segundos % 60)
        return f"{minutos} minutos e {segs} segundos"
    else:
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        return f"{horas} horas e {minutos} minutos"


def executar_script(script_name):
    """
    Executa um script Python e captura sua saída

    Args:
        script_name (str): Nome do script a ser executado

    Raises:
        Exception: Se o script falhar
    """
    # Verificar se o script existe
    if not os.path.exists(script_name):
        erro_msg = f"Script não encontrado: {script_name}"
        logger.error(erro_msg)
        print(f"\n[ERRO] {erro_msg}")
        raise FileNotFoundError(erro_msg)

    logger.info(f"🔹 Executando: {script_name}")
    print(f"\n{'='*80}")
    print(f"Executando: {script_name}")
    print(f"{'='*80}")

    try:
        result = subprocess.run(
            ['python', script_name],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            # Sem timeout por padrão (pode ser configurado se necessário)
            timeout=None
        )

        # Exibir saída do script
        if result.stdout:
            print(result.stdout)
            # Log da saída também
            logger.debug(f"Saída do script {script_name}:\n{result.stdout}")

        # Exibir stderr se houver (mesmo que não seja erro fatal)
        if result.stderr:
            logger.warning(f"Stderr do script {script_name}:\n{result.stderr}")

        # Verificar se houve erro
        if result.returncode != 0:
            # Combinar stdout e stderr para melhor diagnóstico
            erro_completo = ""
            if result.stdout:
                erro_completo += f"STDOUT:\n{result.stdout}\n"
            if result.stderr:
                erro_completo += f"STDERR:\n{result.stderr}\n"

            erro_msg = f"Erro ao executar {script_name} (código de saída: {result.returncode})"
            if erro_completo:
                erro_msg += f"\n{erro_completo}"
            else:
                erro_msg += "\nNenhuma saída capturada."

            logger.error(erro_msg)
            print(f"\n[ERRO] {erro_msg}")
            raise Exception(erro_msg)

        logger.success(f"✅ Script {script_name} executado com sucesso")
        print(f"\n[OK] Script {script_name} concluido com sucesso!")

    except subprocess.TimeoutExpired:
        erro_msg = f"Timeout ao executar {script_name} (script demorou muito para executar)"
        logger.error(erro_msg)
        print(f"\n[ERRO] {erro_msg}")
        raise Exception(erro_msg)
    except FileNotFoundError:
        erro_msg = f"Python não encontrado ou script não existe: {script_name}"
        logger.error(erro_msg)
        print(f"\n[ERRO] {erro_msg}")
        raise
    except Exception as e:
        logger.error(f"Falha ao executar {script_name}: {str(e)}")
        raise


def executar_camada(nome_camada, scripts, emoji=""):
    """
    Executa todos os scripts de uma camada

    Args:
        nome_camada (str): Nome da camada (ex: "OMIE", "RAW", "SILVER", etc)
        scripts (list): Lista de scripts a executar
        emoji (str): Emoji para representar a camada (usado apenas nos logs)

    Returns:
        dict: Estatísticas da execução da camada (tempo, scripts executados)
    """
    inicio_camada = time.time()

    logger.info(f"{'='*80}")
    logger.info(f"{emoji} INICIANDO CAMADA: {nome_camada}")
    logger.info(f"{'='*80}")

    print(f"\n{'='*80}")
    print(f"CAMADA: {nome_camada}")
    print(f"{'='*80}")
    print(f"Total de scripts: {len(scripts)}")

    # Validar existência dos scripts antes de executar
    scripts_inexistentes = [s for s in scripts if not os.path.exists(s)]
    if scripts_inexistentes:
        erro_msg = f"Scripts não encontrados na camada {nome_camada}: {', '.join(scripts_inexistentes)}"
        logger.error(erro_msg)
        print(f"\n[ERRO] {erro_msg}")
        raise FileNotFoundError(erro_msg)

    for i, script in enumerate(scripts, 1):
        logger.info(f"[{i}/{len(scripts)}] Processando: {script}")
        print(f"\n[{i}/{len(scripts)}] Processando: {script}")
        try:
            executar_script(script)
        except Exception as e:
            # Adicionar contexto sobre qual script falhou
            logger.error(
                f"Falha na camada {nome_camada}, script {script} ({i}/{len(scripts)})")
            raise Exception(
                f"Erro na camada {nome_camada} ao executar {script} ({i}/{len(scripts)}): {str(e)}")

    tempo_camada = time.time() - inicio_camada
    tempo_formatado_camada = formatar_tempo(tempo_camada)

    logger.success(f"✅ Camada {nome_camada} concluída com sucesso!")
    logger.info(f"⏱️  Tempo da camada {nome_camada}: {tempo_formatado_camada}")
    print(f"\n{'='*80}")
    print(f"[OK] Camada {nome_camada} concluida!")
    print(f"Tempo da camada: {tempo_formatado_camada}")
    print(f"{'='*80}\n")

    return {
        "tempo_segundos": tempo_camada,
        "tempo_formatado": tempo_formatado_camada,
        "scripts_executados": len(scripts)
    }


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """
    Executa o pipeline ETL completo:
    OMIE → RAW → SILVER → GOLD → MODELO SEMÂNTICO
    """
    inicio = time.time()
    scripts_executados = 0
    total_scripts = (
        len(scripts_omie) +
        len(scripts_raw) +
        len(scripts_silver) +
        len(scripts_gold) +
        len(scripts_modelo_semantico)
    )

    # Dicionário para armazenar estatísticas por camada
    estatisticas_camadas = {}

    try:
        logger.info("="*80)
        logger.info(
            "🚀 INICIANDO PIPELINE ETL COMPLETO - MACHTOOLS PROJETO VENDAS")
        logger.info("="*80)
        logger.info(
            f"Data/Hora início: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        logger.info(f"Total de scripts a executar: {total_scripts}")
        logger.info("="*80)

        print("\n" + "="*80)
        print("PIPELINE ETL MACHTOOLS - PROJETO VENDAS")
        print("="*80)
        print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Total de scripts: {total_scripts}")
        print("="*80)

        # CAMADA OMIE
        estatisticas_camadas["OMIE"] = executar_camada(
            "OMIE", scripts_omie, "🔵")
        scripts_executados += len(scripts_omie)

        # CAMADA RAW
        estatisticas_camadas["RAW"] = executar_camada("RAW", scripts_raw, "🥉")
        scripts_executados += len(scripts_raw)

        # CAMADA SILVER
        estatisticas_camadas["SILVER"] = executar_camada(
            "SILVER", scripts_silver, "🥈")
        scripts_executados += len(scripts_silver)

        # CAMADA GOLD
        estatisticas_camadas["GOLD"] = executar_camada(
            "GOLD", scripts_gold, "🥇")
        scripts_executados += len(scripts_gold)

        # MODELO SEMÂNTICO
        estatisticas_camadas["MODELO_SEMANTICO"] = executar_camada(
            "MODELO SEMÂNTICO", scripts_modelo_semantico, "📊")
        scripts_executados += len(scripts_modelo_semantico)

        # Calcular tempo total
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        # Log de sucesso
        logger.info("="*80)
        logger.success("🎉 PIPELINE ETL FINALIZADO COM SUCESSO!")
        logger.info("="*80)
        logger.info(
            f"✅ Scripts executados: {scripts_executados}/{total_scripts}")
        logger.info(f"⏱️  Tempo total de execução: {tempo_formatado}")
        logger.info(
            f"📅 Data/Hora conclusão: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        logger.info("="*80)

        print("\n" + "="*80)
        print("PIPELINE ETL FINALIZADO COM SUCESSO!")
        print("="*80)
        print(f"Scripts executados: {scripts_executados}/{total_scripts}")
        print(f"Tempo total: {tempo_formatado}")
        print(f"Conclusao: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print("="*80)

        # Preparar diagnóstico para email com estatísticas detalhadas
        diagnostico = {
            "Status": "✅ Todos os scripts executados com sucesso",
            "Scripts Executados": f"{scripts_executados}/{total_scripts}",
            "Camada OMIE": f"{len(scripts_omie)} scripts - {estatisticas_camadas.get('OMIE', {}).get('tempo_formatado', 'N/A')}",
            "Camada RAW": f"{len(scripts_raw)} scripts - {estatisticas_camadas.get('RAW', {}).get('tempo_formatado', 'N/A')}",
            "Camada SILVER": f"{len(scripts_silver)} scripts - {estatisticas_camadas.get('SILVER', {}).get('tempo_formatado', 'N/A')}",
            "Camada GOLD": f"{len(scripts_gold)} scripts - {estatisticas_camadas.get('GOLD', {}).get('tempo_formatado', 'N/A')}",
            "Modelo Semântico": f"{len(scripts_modelo_semantico)} script(s) - {estatisticas_camadas.get('MODELO_SEMANTICO', {}).get('tempo_formatado', 'N/A')}",
            "Tempo Total": tempo_formatado,
            "Data/Hora Conclusão": datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        }

        # Log de estatísticas por camada
        logger.info("="*80)
        logger.info("📊 ESTATÍSTICAS POR CAMADA:")
        logger.info("="*80)
        for camada, stats in estatisticas_camadas.items():
            logger.info(
                f"  {camada}: {stats.get('tempo_formatado', 'N/A')} ({stats.get('scripts_executados', 0)} scripts)")
        logger.info("="*80)

        logger.success("="*80)
        logger.success("✅ PROCESSO COMPLETO FINALIZADO COM SUCESSO!")
        logger.success("="*80)

        print("\n" + "="*80)
        print("[OK] PROCESSO COMPLETO FINALIZADO!")
        print("="*80 + "\n")

    except KeyboardInterrupt:
        # Calcular tempo até a interrupção
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        logger.warning("="*80)
        logger.warning("⚠️ PIPELINE ETL INTERROMPIDO PELO USUÁRIO!")
        logger.warning("="*80)
        logger.warning(
            f"Scripts executados antes da interrupção: {scripts_executados}/{total_scripts}")
        logger.warning(f"Tempo até a interrupção: {tempo_formatado}")
        logger.warning("="*80)

        print("\n" + "="*80)
        print("[AVISO] PIPELINE ETL INTERROMPIDO PELO USUÁRIO!")
        print("="*80)
        print(f"Scripts executados: {scripts_executados}/{total_scripts}")
        print(f"Tempo ate a interrupcao: {tempo_formatado}")
        print("="*80 + "\n")

        raise

    except Exception as e:
        # Calcular tempo até o erro
        tempo_total = time.time() - inicio
        tempo_formatado = formatar_tempo(tempo_total)

        # Capturar traceback completo
        erro_completo = traceback.format_exc()

        # Identificar qual camada estava sendo executada
        camada_atual = "Desconhecida"
        if scripts_executados < len(scripts_omie):
            camada_atual = "OMIE"
        elif scripts_executados < len(scripts_omie) + len(scripts_raw):
            camada_atual = "RAW"
        elif scripts_executados < len(scripts_omie) + len(scripts_raw) + len(scripts_silver):
            camada_atual = "SILVER"
        elif scripts_executados < len(scripts_omie) + len(scripts_raw) + len(scripts_silver) + len(scripts_gold):
            camada_atual = "GOLD"
        else:
            camada_atual = "MODELO SEMÂNTICO"

        # Log de erro
        logger.error("="*80)
        logger.error("❌ PIPELINE ETL FINALIZADO COM ERRO!")
        logger.error("="*80)
        logger.error(f"Camada onde ocorreu o erro: {camada_atual}")
        logger.error(
            f"Scripts executados antes do erro: {scripts_executados}/{total_scripts}")
        logger.error(f"Tempo até o erro: {tempo_formatado}")
        logger.error(f"Erro: {str(e)}")
        logger.error("="*80)
        logger.error("Traceback completo:")
        logger.error(erro_completo)
        logger.error("="*80)

        print("\n" + "="*80)
        print("[ERRO] PIPELINE ETL FINALIZADO COM ERRO!")
        print("="*80)
        print(f"Camada onde ocorreu o erro: {camada_atual}")
        print(f"Scripts executados: {scripts_executados}/{total_scripts}")
        print(f"Tempo ate o erro: {tempo_formatado}")
        print(f"Erro: {str(e)}")
        print("="*80)

        # Preparar mensagem de erro mais informativa para o email
        erro_email = f"""
Camada onde ocorreu o erro: {camada_atual}
Scripts executados antes do erro: {scripts_executados}/{total_scripts}
Tempo até o erro: {tempo_formatado}
Data/Hora do erro: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}

Erro:
{str(e)}

Traceback completo:
{erro_completo}
"""

        print("\n[ERRO] PROCESSO FINALIZADO COM ERRO!")
        print("="*80 + "\n")

        # Re-lançar a exceção para que o código de saída seja diferente de 0
        raise


# =============================================================================
# EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    import sys

    print("\n" + "="*80)
    print("PIPELINE ETL MACHTOOLS - PROJETO VENDAS")
    print("="*80)
    print("Arquitetura: OMIE → RAW → SILVER → GOLD → Modelo Semântico")
    print("="*80 + "\n")

    try:
        main()
        sys.exit(0)  # Sucesso
    except KeyboardInterrupt:
        print("\n\n[AVISO] Execucao interrompida pelo usuario (Ctrl+C)")
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        print("\n\n[ERRO] Execucao finalizada com erro. Verifique os logs acima.")
        logger.error(f"Erro final: {str(e)}")
        sys.exit(1)
