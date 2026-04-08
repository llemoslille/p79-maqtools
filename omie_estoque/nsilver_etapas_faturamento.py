import pandas as pd
import os
from datetime import datetime

# Caminhos dos arquivos
ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/etapas_faturamento/bronze_etapas_faturamento.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/etapas_faturamento/silver_etapas_faturamento.parquet'

# Garante que o diretório de saída existe
os.makedirs('maq_prod_estoque/silver/etapas_faturamento', exist_ok=True)


def main():
    print("[PROCESSANDO] Iniciando processamento SILVER - Etapas de Faturamento")

    # Verifica se o arquivo bronze existe
    if not os.path.exists(ARQUIVO_BRONZE):
        print(f"[ERRO] Arquivo bronze não encontrado: {ARQUIVO_BRONZE}")
        return

    # Carrega dados bronze
    df_bronze = pd.read_parquet(ARQUIVO_BRONZE)
    print(f"[INFO] Registros carregados: {len(df_bronze)}")

    # Seleciona colunas relevantes para silver
    colunas_desejadas = [
        'cCodOperacao', 'cDescOperacao', 'cCodEtapa', 'cDescEtapa',
        'nCodEtapa', 'nCodOperacao', '_pagina_coleta', '_data_coleta'
    ]

    # Filtra apenas colunas que existem
    colunas_existentes = [
        col for col in colunas_desejadas if col in df_bronze.columns]
    print(f"[INFO] Colunas selecionadas: {colunas_existentes}")

    # Cria DataFrame silver
    df_silver = df_bronze[colunas_existentes].copy()
    print(f"[INFO] Registros na silver: {len(df_silver)}")

    # Adiciona metadados
    df_silver['data_processamento'] = pd.Timestamp.now()
    df_silver['camada'] = 'silver'
    df_silver['fonte'] = 'omie_api'

    # Remove duplicatas
    df_silver = df_silver.drop_duplicates()

    # Salva arquivo silver
    df_silver.to_parquet(ARQUIVO_SILVER, index=False)
    print(f"[OK] Arquivo SILVER gerado: {ARQUIVO_SILVER}")
    print(f"[INFO] Total de registros: {len(df_silver)}")

    # Mostra amostra dos dados
    print("\n[AMOSTRA] Amostra dos dados:")
    print(df_silver.head())

    # Estatísticas
    print(f"\n[ESTATISTICAS]")
    print(f"- Total de registros: {len(df_silver)}")
    print(f"- Operações únicas: {df_silver['cCodOperacao'].nunique()}")

    # Verifica se a coluna cCodEtapa existe antes de usá-la
    if 'cCodEtapa' in df_silver.columns:
        print(f"- Etapas únicas: {df_silver['cCodEtapa'].nunique()}")
    else:
        print(f"- Etapas únicas: N/A (coluna não encontrada)")

    print("\n[SUCESSO] Processamento SILVER concluído com sucesso!")


if __name__ == "__main__":
    main()
