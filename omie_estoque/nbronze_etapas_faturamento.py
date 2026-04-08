import requests
import json
import pandas as pd
import os
from datetime import datetime
import time
from novo_projeto.config import APP_KEY, APP_SECRET

# URL da API
URL = "https://app.omie.com.br/api/v1/produtos/etapafat/"

# Arquivo de saída
ARQUIVO_BRONZE = "maq_prod_estoque/bronze/etapas_faturamento/bronze_etapas_faturamento.parquet"


def fazer_requisicao(pagina=1, registros_por_pagina=50):
    """Faz requisição para a API de etapas de faturamento"""

    payload = {
        "call": "ListarEtapasFaturamento",
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina
        }],
        "app_key": APP_KEY,
        "app_secret": APP_SECRET
    }

    try:
        response = requests.post(URL, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERRO] Erro na requisição: {e}")
        return None


def processar_etapas_faturamento():
    """Processa todas as etapas de faturamento com paginação"""
    print("[PROCESSANDO] Iniciando coleta de etapas de faturamento...")
    todas_etapas = []
    pagina = 1
    registros_por_pagina = 50
    while True:
        print(f"[PAGINA] Coletando página {pagina}...")
        response = fazer_requisicao(pagina, registros_por_pagina)
        if not response:
            print(f"[ERRO] Falha na página {pagina}")
            break
        # O retorno correto é 'cadastros', cada um com 'etapas'
        cadastros = response.get('cadastros', [])
        if not cadastros:
            print(f"[OK] Fim dos dados na página {pagina}")
            break
        etapas_encontradas = 0
        for cadastro in cadastros:
            if not cadastro or 'etapas' not in cadastro:
                continue
            for etapa in cadastro['etapas']:
                etapa_completa = etapa.copy()
                etapa_completa['cCodOperacao'] = cadastro.get('cCodOperacao')
                etapa_completa['cDescOperacao'] = cadastro.get('cDescOperacao')
                etapa_completa['_pagina_coleta'] = pagina
                etapa_completa['_data_coleta'] = datetime.now().isoformat()
                etapa_completa['_timestamp_coleta'] = datetime.now(
                ).timestamp()
                todas_etapas.append(etapa_completa)
                etapas_encontradas += 1
        print(
            f"[INFO] {etapas_encontradas} etapas coletadas na página {pagina}")
        if len(cadastros) < registros_por_pagina:
            print(
                f"[OK] Última página atingida (menos de {registros_por_pagina} cadastros)")
            break
        pagina += 1
        time.sleep(0.5)
    return todas_etapas


def salvar_bronze(etapas):
    """Salva os dados na camada bronze"""

    if not etapas:
        print("[ERRO] Nenhuma etapa de faturamento para salvar")
        return False

    print(f"[SALVANDO] Salvando {len(etapas)} etapas de faturamento...")

    # Converte para DataFrame
    df = pd.DataFrame(etapas)

    # Converte listas e dicionários para JSON string para evitar problemas no Parquet
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: json.dumps(
                x) if isinstance(x, (list, dict)) else x)

    # Cria diretório se não existir
    os.makedirs(os.path.dirname(ARQUIVO_BRONZE), exist_ok=True)

    # Salva arquivo
    df.to_parquet(ARQUIVO_BRONZE, index=False)

    print(f"[OK] Arquivo BRONZE salvo: {ARQUIVO_BRONZE}")
    print(f"[INFO] Total de registros: {len(df)}")
    print(f"[INFO] Colunas: {list(df.columns)}")

    # Mostra amostra dos dados
    print("\n[AMOSTRA] Amostra dos dados:")
    print(df.head(3))

    return True


def main():
    """Função principal"""

    print("[INICIO] Iniciando coleta de etapas de faturamento - Camada BRONZE")
    print("=" * 60)

    # Coleta os dados
    etapas = processar_etapas_faturamento()

    if etapas:
        # Salva na camada bronze
        sucesso = salvar_bronze(etapas)

        if sucesso:
            print("\n[SUCESSO] Processamento concluído com sucesso!")
        else:
            print("\n[ERRO] Falha ao salvar dados")
    else:
        print("\n[ERRO] Nenhum dado coletado")


if __name__ == "__main__":
    main()
