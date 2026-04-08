import pandas as pd

ARQUIVO_SILVER = 'maq_prod_estoque/silver/pedidos/silver_det_produtos_pedido.parquet'
ARQUIVO_REPETIDOS = 'maq_prod_estoque/silver/pedidos/silver_pedidos_repetidos.parquet'


def main():
    df = pd.read_parquet(ARQUIVO_SILVER)
    
    # Verifica se a coluna codigo_pedido existe (coluna correta do arquivo)
    if 'codigo_pedido' not in df.columns:
        print(f"[ERRO] Coluna 'codigo_pedido' não encontrada!")
        print(f"Colunas disponíveis: {list(df.columns)}")
        return
    
    contagem = df['codigo_pedido'].value_counts()
    pedidos_repetidos = contagem[contagem > 1].index.tolist()
    df_repetidos = df[df['codigo_pedido'].isin(pedidos_repetidos)]
    
    df_repetidos.to_parquet(ARQUIVO_REPETIDOS, index=False)
    print(f"[OK] Arquivo de pedidos repetidos gerado: {ARQUIVO_REPETIDOS}")
    print(f"Pedidos repetidos: {pedidos_repetidos}")
    print(f"Total de registros repetidos: {len(df_repetidos)}")
    print(df_repetidos.head())


if __name__ == '__main__':
    main()
