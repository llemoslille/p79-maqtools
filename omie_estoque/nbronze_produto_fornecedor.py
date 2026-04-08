import requests
import pandas as pd
import os
import time
from novo_projeto.config import APP_KEY, APP_SECRET

ENDPOINT = 'https://app.omie.com.br/api/v1/estoque/produtofornecedor/'
ARQUIVO_SAIDA = 'maq_prod_estoque/bronze/produto_servico_fornecedor/bronze_produto_fornecedor.parquet'
os.makedirs('maq_prod_estoque/bronze/produto_servico_fornecedor', exist_ok=True)


def listar_produto_fornecedor(pagina=1, registros_por_pagina=50, max_retries=3):
    payload = {
        "call": "ListarProdutoFornecedor",
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina
        }],
        "app_key": APP_KEY,
        "app_secret": APP_SECRET
    }

    for tentativa in range(max_retries):
        try:
            response = requests.post(
                ENDPOINT, json=payload, timeout=60)  # Aumentado timeout
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(
                f"Timeout na página {pagina} (tentativa {tentativa + 1}/{max_retries})")
            if tentativa < max_retries - 1:
                time.sleep(5)  # Espera 5 segundos antes de tentar novamente
            else:
                raise
        except requests.exceptions.RequestException as e:
            print(
                f"Erro na página {pagina} (tentativa {tentativa + 1}/{max_retries}): {e}")
            if tentativa < max_retries - 1:
                time.sleep(5)
            else:
                raise


def main():
    pagina = 1
    resultados = []
    total_de_paginas = None
    paginas_puladas = []

    while True:
        try:
            print(f"Consultando página {pagina}...")
            resp = listar_produto_fornecedor(pagina=pagina)

            if total_de_paginas is None:
                total_de_paginas = resp.get('total_de_paginas', 1)

            cadastros = resp.get('cadastros', [])
            for fornecedor in cadastros:
                base = {
                    'cCodIntForn': fornecedor.get('cCodIntForn', ''),
                    'cCpfCnpj': fornecedor.get('cCpfCnpj', ''),
                    'cNomeFantasia': fornecedor.get('cNomeFantasia', ''),
                    'cRazaoSocial': fornecedor.get('cRazaoSocial', ''),
                    'nCodForn': fornecedor.get('nCodForn', 0)
                }
                produtos = fornecedor.get('produtos', [])
                if produtos:
                    for prod in produtos:
                        linha = base.copy()
                        linha.update({
                            'cCodIntProd': prod.get('cCodIntProd', ''),
                            'cCodigo': prod.get('cCodigo', ''),
                            'cDescricao': prod.get('cDescricao', ''),
                            'nCodProd': prod.get('nCodProd', 0)
                        })
                        resultados.append(linha)
                else:
                    # Caso fornecedor não tenha produtos, ainda salva info do fornecedor
                    resultados.append(base)

            if pagina >= total_de_paginas:
                break
            pagina += 1

            # Pequena pausa entre requisições para evitar sobrecarga
            time.sleep(1)

        except Exception as e:
            print(f"Erro na página {pagina}: {e}")
            paginas_puladas.append(pagina)
            pagina += 1
            if pagina > total_de_paginas if total_de_paginas else 100:  # Limite de segurança
                break

    if resultados:
        df = pd.DataFrame(resultados)
        print(f"Registros coletados: {len(df)}")
        if paginas_puladas:
            print(f"Páginas puladas: {paginas_puladas}")

        df.to_parquet(ARQUIVO_SAIDA, index=False)
        print(f"[OK] Bronze produto_fornecedor gerado: {ARQUIVO_SAIDA}")
        print(df.head())
    else:
        print("Nenhum registro encontrado.")


if __name__ == "__main__":
    main()
