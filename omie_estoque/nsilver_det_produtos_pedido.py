import pandas as pd
import os

ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/pedidos/bronze_pedidos_full.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/pedidos/silver_det_produtos_pedido.parquet'
os.makedirs('maq_prod_estoque/silver/pedidos', exist_ok=True)


def expandir_colunas_lista(df):
    cols = list(df.columns)
    for col in cols:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df = df.explode(col, ignore_index=True)
            df = df.drop(columns=[col])
    return df


def main():
    df = pd.read_parquet(ARQUIVO_BRONZE)
    df = expandir_colunas_lista(df)
    df.to_parquet(ARQUIVO_SILVER, index=False)
    print(f"[OK] Silver gerado: {ARQUIVO_SILVER}")
    print(f"Registros: {len(df)}")
    print(df.head())


if __name__ == '__main__':
    main()
