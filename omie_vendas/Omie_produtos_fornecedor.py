import requests
import json
import csv
import os
import io
from datetime import datetime
from google.cloud import storage

# Configurações da API
ENDPOINT = "https://app.omie.com.br/api/v1/estoque/produtofornecedor/"
APP_KEY = "1733209266789"
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"

# Configuração do Google Cloud Storage
GCS_BUCKET_NAME = "maq_vendas"
GCS_DESTINATION_BLOB_NAME = "fornecedores/produtos_fornecedor.csv"


def _resolve_gcp_credentials_path() -> str:
    return (
        os.getenv("GCS_CREDENTIALS_JSON_PATH")
        or os.getenv("MACHTOOLS_JSON_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "machtools.json"
    )


GCS_CREDENTIALS_PATH = _resolve_gcp_credentials_path()

# Verifica se o arquivo de credenciais do GCS existe
if not os.path.exists(GCS_CREDENTIALS_PATH):
    raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {GCS_CREDENTIALS_PATH}")

# Configura o ambiente com as credenciais do GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

def listar_produtos_fornecedor(pagina, registros_por_pagina):
    params = {
        "call": "ListarProdutoFornecedor",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina,
            "apenas_importado_api": "N",
            "ordenar_por": "CODIGO",
            "ordem_decrescente": "N"
        }]
    }
    
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(ENDPOINT, json=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Log da resposta da API para depuração
        print(f"\nResposta da API (Página {pagina}):")
        print(f"Total de registros: {data.get('total_de_registros', 0)}")
        print(f"Total de páginas: {data.get('total_de_paginas', 0)}")
        
        if "cadastros" in data:
            return data["cadastros"]
        else:
            print(f"Resposta inesperada: {data}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a página {pagina}: {e}")
        return None

def gerar_csv_e_enviar_para_gcs(fornecedores):
    colunas = [
        "Código Fornecedor",
        "Código Integração Fornecedor",
        "CPF/CNPJ",
        "Razão Social",
        "Nome Fantasia",
        "Código Produto",
        "Código Integração Produto",
        "Código Fornecedor Produto",
        "Descrição Produto",
        "Data Processamento"
    ]
    
    dados_csv = []
    data_processamento = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    for fornecedor in fornecedores:
        # Log do fornecedor para depuração
        print(f"Processando fornecedor: {json.dumps(fornecedor, indent=2)}")
        
        # Se o fornecedor tiver produtos, processa cada um
        if "produtos" in fornecedor and fornecedor["produtos"]:
            for produto in fornecedor["produtos"]:
                dados_csv.append({
                    "Código Fornecedor": fornecedor.get("nCodForn", ""),
                    "Código Integração Fornecedor": fornecedor.get("cCodIntForn", ""),
                    "CPF/CNPJ": fornecedor.get("cCpfCnpj", ""),
                    "Razão Social": fornecedor.get("cRazaoSocial", ""),
                    "Nome Fantasia": fornecedor.get("cNomeFantasia", ""),
                    "Código Produto": produto.get("nCodProd", ""),
                    "Código Integração Produto": produto.get("cCodIntProd", ""),
                    "Código Fornecedor Produto": produto.get("cCodigo", ""),
                    "Descrição Produto": produto.get("cDescricao", ""),
                    "Data Processamento": data_processamento
                })
        else:
            # Adiciona o fornecedor mesmo sem produtos
            dados_csv.append({
                "Código Fornecedor": fornecedor.get("nCodForn", ""),
                "Código Integração Fornecedor": fornecedor.get("cCodIntForn", ""),
                "CPF/CNPJ": fornecedor.get("cCpfCnpj", ""),
                "Razão Social": fornecedor.get("cRazaoSocial", ""),
                "Nome Fantasia": fornecedor.get("cNomeFantasia", ""),
                "Código Produto": "",
                "Código Integração Produto": "",
                "Código Fornecedor Produto": "",
                "Descrição Produto": "",
                "Data Processamento": data_processamento
            })

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)

        with io.StringIO() as output:
            writer = csv.DictWriter(output, fieldnames=colunas, delimiter=";")
            writer.writeheader()
            writer.writerows(dados_csv)
            blob.upload_from_string(output.getvalue(), content_type="text/csv")
        
        print(f"\n{'='*50}")
        print(f"ARQUIVO SALVO COM SUCESSO NO GCS!")
        print(f"Caminho: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")
        print(f"Total de registros: {len(dados_csv)}")
        print(f"Data de processamento: {data_processamento}")
        print(f"{'='*50}\n")
    except Exception as e:
        print(f"Erro ao enviar o arquivo CSV para o Google Cloud Storage: {e}")

def processar_produtos_fornecedor():
    pagina = 1
    registros_por_pagina = 50
    todos_fornecedores = []
    
    while True:
        print(f"\nProcessando página {pagina}...")
        fornecedores = listar_produtos_fornecedor(pagina, registros_por_pagina)
        
        if fornecedores is None or not fornecedores:
            print(f"Não há mais fornecedores para processar.")
            break
            
        todos_fornecedores.extend(fornecedores)
        print(f"Página {pagina} processada com sucesso. Fornecedores encontrados: {len(fornecedores)}")
        pagina += 1

    if todos_fornecedores:
        gerar_csv_e_enviar_para_gcs(todos_fornecedores)
        print(f"\nResumo do processamento:")
        print(f"Total de fornecedores processados: {len(todos_fornecedores)}")
    else:
        print("Nenhum fornecedor válido foi processado.")

if __name__ == "__main__":
    processar_produtos_fornecedor() 