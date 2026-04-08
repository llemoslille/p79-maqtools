import requests
import pandas as pd
from datetime import datetime
import os
import json
from novo_projeto.config import APP_KEY, APP_SECRET

# Configurações
ENDPOINT = 'https://app.omie.com.br/api/v1/produtos/pedido/'
ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/pedidos/bronze_pedidos_full.parquet'
os.makedirs('maq_prod_estoque/bronze/pedidos', exist_ok=True)

REGISTROS_POR_PAGINA = 50


def coletar_pedidos(pagina=1, registros_por_pagina=50):
    payload = {
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "call": "ListarPedidos",
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina,
            "apenas_importado_api": "N"
        }]
    }
    response = requests.post(ENDPOINT, json=payload, timeout=30)
    try:
        response.raise_for_status()
    except Exception as e:
        print(f"Erro na requisição: {e}")
        print(f"Status code: {response.status_code}")
        print(f"Resposta da API: {response.text}")
        raise
    return response.json()


def processar_pedido(pedido):
    cabecalho = pedido.get('cabecalho', {})
    # Campos complexos para JSON
    campos_json = ['det', 'exportacao', 'frete', 'infoCadastro',
                   'informacoes_adicionais', 'lista_parcelas', 'total_pedido']
    dados = {**cabecalho}
    for campo in campos_json:
        valor = pedido.get(campo, None)
        try:
            dados[campo] = json.dumps(valor, ensure_ascii=False)
        except Exception:
            dados[campo] = str(valor)
    return dados


def main():
    pagina = 1
    todos_pedidos = []
    try:
        resposta = coletar_pedidos(
            pagina=pagina,
            registros_por_pagina=REGISTROS_POR_PAGINA
        )
        if 'pedido_venda_produto' in resposta:
            pedidos = resposta['pedido_venda_produto']
            if isinstance(pedidos, list):
                for pedido in pedidos:
                    dados = processar_pedido(pedido)
                    todos_pedidos.append(dados)
            elif isinstance(pedidos, dict):
                dados = processar_pedido(pedidos)
                todos_pedidos.append(dados)
        else:
            print("Chave 'pedido_venda_produto' não encontrada na resposta.")
    except Exception as e:
        print(f"Erro ao coletar pedidos: {e}")
    if todos_pedidos:
        df = pd.DataFrame(todos_pedidos)
        # Remove colunas problemáticas para Parquet

        def coluna_invalida(serie):
            return serie.apply(lambda x: (x is None) or (isinstance(x, dict) and not x) or (isinstance(x, list) and len(x) == 0)).all()
        cols_para_remover = [
            col for col in df.columns if coluna_invalida(df[col])]
        # Remove colunas do tipo object que não sejam string, número ou bool
        for col in df.select_dtypes(include=['object']).columns:
            tipos = df[col].dropna().map(type).unique()
            if any([(t not in [str, int, float, bool]) for t in tipos]):
                cols_para_remover.append(col)
        if cols_para_remover:
            print(f'Removendo colunas problemáticas: {set(cols_para_remover)}')
            df = df.drop(columns=list(set(cols_para_remover)))
        df['data_coleta'] = datetime.now()
        df['fonte'] = 'omie_api'
        df['camada'] = 'bronze'
        df.to_parquet(ARQUIVO_BRONZE, index=False)
        print(f"[OK] Bronze de pedidos gerado! Arquivo: {ARQUIVO_BRONZE}")
        print(f"Registros coletados: {len(df)}")
        print(f"Colunas: {list(df.columns)}")
        print(df.head())
    else:
        print("Nenhum pedido encontrado para o teste.")


if __name__ == "__main__":
    main()
