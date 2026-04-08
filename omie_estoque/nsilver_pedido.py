import pandas as pd
import json
import os
from datetime import datetime

ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/pedidos/bronze_pedidos_full.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/pedidos/silver_pedido.parquet'
os.makedirs('maq_prod_estoque/silver/pedidos', exist_ok=True)


def limpar_colunas_vazias(df):
    def coluna_totalmente_vazia(serie):
        return serie.apply(lambda x: (x is None) or (isinstance(x, dict) and not x) or (isinstance(x, list) and len(x) == 0)).all()
    cols_para_remover = [
        col for col in df.columns if coluna_totalmente_vazia(df[col])]
    if cols_para_remover:
        print(f'Removendo colunas totalmente vazias: {cols_para_remover}')
        df = df.drop(columns=cols_para_remover)
    return df


def expandir_json_coluna(df, coluna):
    # Converte string JSON para lista/dict
    df[coluna] = df[coluna].apply(lambda x: json.loads(
        x) if pd.notnull(x) and x not in ['', 'null', None] else [])
    # Explode a lista
    df = df.explode(coluna, ignore_index=True)
    # Expande o dicionário interno (se não for vazio)
    if df[coluna].apply(lambda x: isinstance(x, dict)).any():
        dict_exp = df[coluna].apply(
            lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
        dict_exp = dict_exp.add_prefix(f'{coluna}_')
        df = pd.concat([df.drop(columns=[coluna]), dict_exp], axis=1)
    df = limpar_colunas_vazias(df)
    return df


def main():
    df = pd.read_parquet(ARQUIVO_BRONZE)
    # Colunas que são listas em JSON: 'det', 'lista_parcelas'
    for coluna in ['det', 'lista_parcelas']:
        if coluna in df.columns:
            df = expandir_json_coluna(df, coluna)
    # Expande infoCadastro, informacoes_adicionais, total_pedido, frete, exportacao (dicionários)
    for col_dict in ['infoCadastro', 'informacoes_adicionais', 'total_pedido', 'frete', 'exportacao']:
        if col_dict in df.columns:
            df[col_dict] = df[col_dict].apply(lambda x: json.loads(
                x) if pd.notnull(x) and x not in ['', 'null', None] else {})
            dict_exp = df[col_dict].apply(pd.Series).add_prefix(f'{col_dict}_')
            df = pd.concat([df.drop(columns=[col_dict]), dict_exp], axis=1)
            df = limpar_colunas_vazias(df)
    # Adiciona metadados
    df['data_processamento'] = datetime.now()
    df['camada'] = 'silver'
    # Remove colunas onde pelo menos um valor é dict (Parquet não suporta)
    cols_dict = [col for col in df.columns if df[col].apply(
        lambda x: isinstance(x, dict)).any()]
    if cols_dict:
        print(f'Removendo colunas com dict: {cols_dict}')
        df = df.drop(columns=cols_dict)
    df = limpar_colunas_vazias(df)
    df.to_parquet(ARQUIVO_SILVER, index=False)
    df.head(20).to_csv(
        'maq_prod_estoque/silver/pedidos/silver_pedido_amostra.csv', index=False)
    print(f"[OK] Silver de pedido gerado! Arquivo: {ARQUIVO_SILVER}")
    print(f"Amostra salva em: maq_prod_estoque/silver/pedidos/silver_pedido_amostra.csv")
    print(f"Shape: {df.shape}")
    print(f"Colunas: {list(df.columns)}")
    print(df.head())


if __name__ == "__main__":
    main()
