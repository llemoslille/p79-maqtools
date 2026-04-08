import requests
import json
import csv
import io
import os
from google.cloud import storage
from configuracoes.setup_logging import get_logger
# from config import APP_KEY, APP_SECRET

# Configurar logging
logger = get_logger("Omie_produtos.py")

# Configurações da API
ENDPOINT = "https://app.omie.com.br/api/v1/geral/produtos/"

# Configuração do Google Cloud Storage
GCS_BUCKET_NAME = "maq_vendas"
GCS_DESTINATION_BLOB_NAME = "produtos/dimensao_produtos.csv"


def _resolve_gcp_credentials_path() -> str:
    return (
        os.getenv("GCS_CREDENTIALS_JSON_PATH")
        or os.getenv("MACHTOOLS_JSON_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "machtools.json"
    )


GCS_CREDENTIALS_PATH = _resolve_gcp_credentials_path()

# Configura o ambiente com as credenciais do GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

def listar_produtos(pagina, registros_por_pagina):
    params = {
        "call": "ListarProdutos",
        "app_key": "1733209266789", # //APP_KEY,
        "app_secret": "14c2f271c839dfea25cfff4afb63b331", # //APP_SECRET,
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina,
            "apenas_importado_api": "N",
            "filtrar_apenas_omiepdv": "N"
        }]
    }
    
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(ENDPOINT, json=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Log da resposta da API para depuração
        print(f"Resposta da API (Página {pagina}): {json.dumps(data, indent=2)}")
        
        if "produto_servico_cadastro" in data:
            return data["produto_servico_cadastro"]
        else:
            print(f"Resposta inesperada: {data}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a página {pagina}: {e}")
        return None

def gerar_csv_e_enviar_para_gcs(produtos, bucket_name, destino_bucket):
    # Adicionando a coluna "Família" ao CSV
    colunas = ["Código", "Descrição", "Código Interno", "Unidade", "Valor Unitário", "Marca", "Família", "id_produto", "tipo_item"]
    
    dados_csv = []
    for produto in produtos:
        # Log do produto para depuração
        print(f"Processando produto: {json.dumps(produto, indent=2)}")
        
        dados_csv.append({
            "Código": produto.get("codigo", "Não informado"),
            "Descrição": produto.get("descricao", "Não informado"),
            "Código Interno": produto.get("codigo_produto_integracao", "Não informado"),
            "Unidade": produto.get("unidade", "Não informado"),
            "Valor Unitário": produto.get("valor_unitario", "Não informado"),
            "Marca": produto.get("marca", "Não informado"),
            "Família": produto.get("descricao_familia", "Não informado"),  # Acessa diretamente o campo descricao_familia
            "id_produto": produto.get("codigo_produto", "Não informado"),
            "tipo_item": produto.get("tipoItem", "Não informado")
        })

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destino_bucket)

        with io.StringIO() as output:
            writer = csv.DictWriter(output, fieldnames=colunas, delimiter=";")
            writer.writeheader()
            writer.writerows(dados_csv)
            blob.upload_from_string(output.getvalue(), content_type="text/csv")
        
        print(f"Arquivo CSV enviado para o bucket '{bucket_name}' como '{destino_bucket}'.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo CSV para o Google Cloud Storage: {e}")

def processar_produtos_e_salvar():
    pagina = 1
    registros_por_pagina = 200
    todos_produtos = []
    
    while True:
        print(f"\nProcessando página {pagina}...")
        produtos = listar_produtos(pagina, registros_por_pagina)
        
        if produtos is None or not produtos:
            print(f"Não há mais produtos para processar.")
            break
            
        todos_produtos.extend(produtos)
        print(f"Página {pagina} processada com sucesso. Produtos encontrados: {len(produtos)}")
        pagina += 1

    if todos_produtos:
        gerar_csv_e_enviar_para_gcs(todos_produtos, GCS_BUCKET_NAME, GCS_DESTINATION_BLOB_NAME)
        print(f"Total de produtos processados: {len(todos_produtos)}")
    else:
        print("Nenhum produto válido foi processado.")

if __name__ == "__main__":
    processar_produtos_e_salvar()