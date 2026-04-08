import pandas as pd
import os
from datetime import datetime

# Caminhos dos arquivos
ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/produto_servico_fornecedor/bronze_servicos.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_servicos.parquet'
os.makedirs('maq_prod_estoque/silver/produto_servico_fornecedor', exist_ok=True)


def expandir_colunas(df):
    # Expande todas as colunas aninhadas
    for col in ['cabecalho', 'descricao', 'impostos', 'info', 'intListar']:
        if col in df.columns:
            df_expandido = pd.json_normalize(df[col])
            # Renomeia as colunas para evitar conflito
            df_expandido.columns = [f'{col}_{c}' for c in df_expandido.columns]
            df = pd.concat([df.drop(columns=[col]), df_expandido], axis=1)
    return df


def main():
    df = pd.read_parquet(ARQUIVO_BRONZE)
    df_silver = expandir_colunas(df)
    colunas_estoque = [
        'codigo',
        'codigo_produto',
        'cmc',
        'qtd_min_estoque_fisico',
        'qtd_pendente',
        'qtd_reservado',
        'qtd_saldo'
    ]
    for col in colunas_estoque:
        df_silver[col] = 0
    df_silver['data_processamento'] = datetime.now()
    df_silver['fonte'] = 'bronze_servicos_full.parquet'
    df_silver['camada'] = 'silver'
    df_silver.to_parquet(ARQUIVO_SILVER, index=False)
    print(f"[OK] Silver de serviços gerado! Arquivo: {ARQUIVO_SILVER}")
    print(f"Registros: {len(df_silver)}")
    print(f"Colunas: {list(df_silver.columns)}")
    print(df_silver.head())


if __name__ == "__main__":
    main()
