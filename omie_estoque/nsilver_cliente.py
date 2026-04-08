#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Silver Layer - Processamento de clientes a partir do bronze_clientes_fornecedores
Expande a coluna 'tags' e gera arquivo parquet para validação
"""

import pandas as pd
import ast
import os
from datetime import datetime
import numpy as np

# Caminho do arquivo bronze
ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/cliente_fornecedor/bronze_clientes_fornecedores_full.parquet'
ARQUIVO_SILVER = 'maq_prod_estoque/silver/cliente_fornecedor/silver_cliente.parquet'

# Garante que o diretório de saída existe
os.makedirs('maq_prod_estoque/silver/cliente_fornecedor', exist_ok=True)


def expandir_tags_em_colunas(tags):
    # Garante lista de strings
    if tags is None:
        return []
    if isinstance(tags, float) and pd.isna(tags):
        return []
    if isinstance(tags, str) and tags.strip() == '':
        return []
    if isinstance(tags, (list, tuple, np.ndarray)):
        return [t.get('tag') if isinstance(t, dict) and 'tag' in t else str(t) for t in tags]
    return []


def main():
    # Carrega o bronze
    df = pd.read_parquet(ARQUIVO_BRONZE)

    # Seleciona as colunas desejadas
    colunas = [
        'cnpj_cpf',
        'codigo_cliente_omie',
        'nome_fantasia',
        'razao_social',
        'tags'
    ]
    df = df[colunas].copy()

    # Expande a coluna tags em lista de strings
    tags_listas = df['tags'].apply(expandir_tags_em_colunas)
    max_tags = tags_listas.apply(len).max()
    # Cria colunas tag_1, tag_2, ...
    tags_df = pd.DataFrame(tags_listas.tolist(), columns=[
                           f'tag_{i+1}' for i in range(max_tags)])
    df_silver = pd.concat([df.drop(columns=['tags']), tags_df], axis=1)

    # Adiciona metadados
    df_silver['data_processamento'] = datetime.now()
    df_silver['camada'] = 'silver'

    # Salva o arquivo parquet
    df_silver.to_parquet(ARQUIVO_SILVER, index=False)
    # Salva uma amostra em CSV para visualização
    df_silver.head(20).to_csv(
        'maq_prod_estoque/silver/cliente_fornecedor/silver_cliente_amostra.csv', index=False)
    print(f"[OK] Silver gerado com sucesso! Arquivo: {ARQUIVO_SILVER}")
    print(f"Amostra salva em: maq_prod_estoque/silver/cliente_fornecedor/silver_cliente_amostra.csv")
    print(f"Shape: {df_silver.shape}")
    print(f"Colunas: {list(df_silver.columns)}")
    print(df_silver.head())


if __name__ == "__main__":
    main()
