import pandas as pd
import os
from datetime import datetime

ARQUIVO_BRONZE_ESTOQUE = 'maq_prod_estoque/bronze/produto_estoque/bronze_produto_estoque.parquet'
ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/produto_servico_fornecedor/bronze_produtos.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_produto.parquet'

# Garante que o diretório de saída existe
os.makedirs('maq_prod_estoque/silver/produto_servico_fornecedor', exist_ok=True)


def expandir_info(info):
    if info is None:
        return {}
    if isinstance(info, float) and pd.isna(info):
        return {}
    if isinstance(info, str):
        try:
            return eval(info)
        except Exception:
            return {}
    if isinstance(info, dict):
        return info
    return {}


def main():
    df = pd.read_parquet(ARQUIVO_BRONZE)
    # Seleciona as colunas principais
    colunas = [
        'codigo',
        'codigo_familia',
        'codigo_produto',
        'descricao',
        'unidade',
        'descricao_familia',
        'info',
        'marca',
        'valor_unitario',
        'tipoItem'
    ]
    df = df[colunas].copy()
    # Expande a coluna info
    info_exp = df['info'].apply(expandir_info).apply(pd.Series)
    df_silver = pd.concat([df.drop(columns=['info']), info_exp], axis=1)
    # Adiciona colunas padronizadas para compatibilidade com serviços
    colunas_padrao = [
        'codigo',
        'codigo_produto',
        'cmc',
        'qtd_min_estoque_fisico',
        'qtd_pendente',
        'qtd_reservado',
        'qtd_saldo'
    ]
    for col in colunas_padrao:
        if col not in df_silver.columns:
            df_silver[col] = 0
    # Adiciona metadados
    df_silver['data_processamento'] = datetime.now()
    df_silver['camada'] = 'silver'

    # --- Diagnóstico: tipos e valores das chaves antes do join ---
    print('\n[DEBUG] Tipo de df_silver["codigo_produto"]:',
          df_silver['codigo_produto'].dtype)
    print('[DEBUG] Tipo de df_estoque["codigo_produto"]: será exibido após leitura do estoque')
    print('[DEBUG] Primeiros valores de df_silver["codigo_produto"]:',
          df_silver['codigo_produto'].head())
    # Salva o arquivo parquet
    # df_silver.to_parquet(ARQUIVO_SILVER, index=False)  # REMOVIDO: não salvar antes da junção
    # Salva amostra em CSV
    # df_silver.head(20).to_csv('data/silver/silver_produto_amostra.csv', index=False)  # REMOVIDO
    # print(f"[OK] Silver de produto gerado com sucesso! Arquivo: {ARQUIVO_SILVER}")  # REMOVIDO
    # print(f"Amostra salva em: data/silver/silver_produto_amostra.csv")  # REMOVIDO
    # print(f"Shape: {df_silver.shape}")  # REMOVIDO
    # print(f"Colunas: {list(df_silver.columns)}")  # REMOVIDO
    # print(df_silver.head())  # REMOVIDO

    # Geração do arquivo gold
    colunas_gold = [
        'codigo',
        'codigo_familia',
        'codigo_produto',
        'descricao',
        'unidade',
        'descricao_familia',
        'marca',
        'valor_unitario'
    ]
    df_gold = df_silver[colunas_gold].copy()
    df_gold = df_gold.rename(columns={'codigo': 'cod_prod'})
    ARQUIVO_GOLD = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_produto.parquet'
    os.makedirs(
        'maq_prod_estoque/gold/servicos_produtos_fornecedor', exist_ok=True)
    df_gold.to_parquet(ARQUIVO_GOLD, index=False)
    print(f"[OK] Gold de produto gerado com sucesso! Arquivo: {ARQUIVO_GOLD}")
    print(f"Colunas gold: {list(df_gold.columns)}")

    # --- Leitura e renomeação do estoque ---
    colunas_estoque = [
        'cCodigo', 'nCodProd', 'fisicoestoque_minimo', 'nCMC', 'nPendente', 'reservado', 'nSaldo'
    ]
    novos_nomes = {
        'cCodigo': 'codigo',
        'nCodProd': 'codigo_produto',
        'nCMC': 'cmc',
        'fisicoestoque_minimo': 'qtd_min_estoque_fisico',
        'nPendente': 'qtd_pendente',
        'reservado': 'qtd_reservado',
        'nSaldo': 'qtd_saldo'
    }
    try:
        try:
            df_estoque = pd.read_parquet(
                ARQUIVO_BRONZE_ESTOQUE, columns=colunas_estoque)
        except Exception:
            df_estoque = pd.read_parquet(ARQUIVO_BRONZE_ESTOQUE)
            colunas_existentes = [
                col for col in colunas_estoque if col in df_estoque.columns]
            df_estoque = df_estoque[colunas_existentes]
        df_estoque = df_estoque.rename(columns=novos_nomes)
        print('\n[INFO] DataFrame de estoque filtrado e renomeado:')
        print(df_estoque.head())
        print('Colunas presentes:', list(df_estoque.columns))
        # Diagnóstico: tipo e valores da chave no estoque
        print('[DEBUG] Tipo de df_estoque["codigo_produto"]:',
              df_estoque['codigo_produto'].dtype)
        print('[DEBUG] Primeiros valores de df_estoque["codigo_produto"]:',
              df_estoque['codigo_produto'].head())
        # Remove do df_estoque as colunas que já existem em df_silver, exceto 'codigo_produto'
        # colunas_duplicadas = [
        #     col for col in df_estoque.columns if col in df_silver.columns and col != 'codigo_produto']
        # df_estoque = df_estoque.drop(columns=colunas_duplicadas)
        # Diagnóstico: interseção dos códigos
        intersecao = set(df_silver['codigo_produto']).intersection(
            set(df_estoque['codigo_produto']))
        print(
            f'[DEBUG] Quantidade de codigos_produto em comum entre silver e estoque: {len(intersecao)}')
        if len(intersecao) > 0:
            print(
                f'[DEBUG] Exemplos de codigos_produto em comum: {list(intersecao)[:10]}')
    except Exception as e:
        print(f'Erro ao criar DataFrame de estoque filtrado: {e}')
        df_estoque = None

    # --- Junção dos DataFrames ---
    if df_estoque is not None:
        df_unido = pd.merge(df_silver, df_estoque,
                            on='codigo_produto', how='left')
        # Sobrescreve as colunas de estoque com os valores do estoque (colunas _y)
        for col in ['cmc', 'qtd_min_estoque_fisico', 'qtd_pendente', 'qtd_reservado', 'qtd_saldo']:
            if f"{col}_y" in df_unido.columns:
                df_unido[col] = df_unido[f"{col}_y"]
                df_unido.drop([f"{col}_x", f"{col}_y"], axis=1,
                              inplace=True, errors='ignore')
        # Garante que só exista uma coluna 'codigo'
        if 'codigo_x' in df_unido.columns and 'codigo_y' in df_unido.columns:
            # ou 'codigo_y' se preferir
            df_unido['codigo'] = df_unido['codigo_x']
            df_unido.drop(['codigo_x', 'codigo_y'], axis=1, inplace=True)
        elif 'codigo_x' in df_unido.columns:
            df_unido.rename(columns={'codigo_x': 'codigo'}, inplace=True)
        elif 'codigo_y' in df_unido.columns:
            df_unido.rename(columns={'codigo_y': 'codigo'}, inplace=True)
        print('\n[INFO] DataFrame unido (silver + estoque):')
        print(df_unido.head())
        print('Colunas do DataFrame unido:', list(df_unido.columns))
        # Salvar resultado FINAL com nome silver_produto
        df_unido.to_parquet(ARQUIVO_SILVER, index=False)
        print(f'[OK] Arquivo final salvo: {ARQUIVO_SILVER}')
    else:
        print('DataFrame de estoque não foi criado, junção não realizada.')


if __name__ == "__main__":
    main()
    # Descomente para rodar a função de estoque filtrado:
    # criar_df_estoque_filtrado()
