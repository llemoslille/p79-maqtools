#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze Layer - Coleta inicial de clientes/fornecedores do Omie
Gera arquivo parquet para validação
"""

import requests
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List
from novo_projeto.config import APP_KEY, APP_SECRET
import os
import time
from logger_config import obter_logger

# Configuração de logging
logger = obter_logger(__name__)


class BronzeClientesFornecedores:
    """Classe para coleta bronze de clientes/fornecedores do Omie"""

    def __init__(self):
        self.app_key = APP_KEY
        self.app_secret = APP_SECRET
        self.base_url = 'https://app.omie.com.br/api/v1/geral/clientes/'
        self.timeout = 60  # Aumentado timeout
        if not self.app_key or not self.app_secret:
            raise ValueError(
                "APP_KEY e APP_SECRET devem estar configurados no config.py")

    def _make_request(self, data: Dict, max_retries: int = 3) -> Dict:
        payload = {
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            **data
        }

        for tentativa in range(max_retries):
            try:
                logger.info(
                    f"Fazendo requisição para: {self.base_url} (tentativa {tentativa + 1}/{max_retries})")
                response = requests.post(
                    self.base_url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.warning(
                    f"Timeout na tentativa {tentativa + 1}/{max_retries}")
                if tentativa < max_retries - 1:
                    import time
                    # Espera 5 segundos antes de tentar novamente
                    time.sleep(5)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Erro na requisição (tentativa {tentativa + 1}/{max_retries}): {e}")
                if tentativa < max_retries - 1:
                    import time
                    time.sleep(5)
                else:
                    raise

    def coletar_todas_paginas(self, registros_por_pagina: int = 50, max_retries: int = 3) -> pd.DataFrame:
        pagina = 1
        todos_clientes = []
        paginas_puladas = []
        while True:
            tentativas = 0
            while tentativas < max_retries:
                try:
                    data = {
                        "call": "ListarClientes",
                        "param": [{
                            "pagina": pagina,
                            "registros_por_pagina": registros_por_pagina,
                            "apenas_importado_api": "N"
                        }]
                    }
                    resposta = self._make_request(data)
                    break
                except Exception as e:
                    tentativas += 1
                    logger.warning(
                        f"Erro na página {pagina} (tentativa {tentativas}/{max_retries}): {e}")
                    if tentativas == max_retries:
                        logger.error(
                            f"Página {pagina} pulada após {max_retries} tentativas.")
                        paginas_puladas.append(pagina)
                        resposta = None
            if resposta and 'clientes_cadastro' in resposta:
                clientes = resposta['clientes_cadastro']
                if not clientes:
                    break
                todos_clientes.extend(clientes)
                total_paginas = resposta.get('total_de_paginas', 1)
                if pagina >= total_paginas:
                    break
                pagina += 1
            else:
                break
        if todos_clientes:
            df = pd.DataFrame(todos_clientes)
            df['data_coleta'] = datetime.now()
            df['fonte'] = 'omie_api'
            df['camada'] = 'bronze'
            if paginas_puladas:
                logger.warning(f"Páginas puladas: {paginas_puladas}")
            return df
        else:
            logger.warning(
                f"Nenhum cliente/fornecedor coletado. Páginas puladas: {paginas_puladas}")
            return pd.DataFrame()


def main():
    try:
        bronze = BronzeClientesFornecedores()
        logger.info(
            "Iniciando coleta bronze de clientes/fornecedores (todas as páginas)...")
        df = bronze.coletar_todas_paginas(registros_por_pagina=50)
        if not df.empty:
            arquivo_parquet = bronze.salvar_parquet(
                df, "bronze_clientes_fornecedores_full.parquet")
            logger.info(f"Colunas do DataFrame: {list(df.columns)}")
            logger.info(f"Primeiras linhas:\n{df.head()}")
            print(f"\n[OK] Validação concluída!")
            print(f"[INFO] Arquivo gerado: {arquivo_parquet}")
            print(f"[INFO] Registros coletados: {len(df)}")
            print(f"[INFO] Colunas: {list(df.columns)}")
        else:
            logger.warning("Nenhum dado foi coletado")
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
        raise


# Reutiliza método de salvar parquet do bronze_produtos
def salvar_parquet_static(df, nome_arquivo=None):
    if nome_arquivo is None:
        nome_arquivo = 'bronze_clientes_fornecedores_full.parquet'

    # Cria diretório se não existir
    os.makedirs('maq_prod_estoque/bronze/cliente_fornecedor', exist_ok=True)

    caminho_arquivo = f"maq_prod_estoque/bronze/cliente_fornecedor/{nome_arquivo}"
    df.to_parquet(caminho_arquivo, index=False)
    return caminho_arquivo


BronzeClientesFornecedores.salvar_parquet = staticmethod(salvar_parquet_static)

if __name__ == "__main__":
    main()
