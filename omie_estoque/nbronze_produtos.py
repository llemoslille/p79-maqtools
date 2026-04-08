#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze Layer - Coleta inicial de dados de produtos do Omie
Gera arquivo parquet para validação
"""

import requests
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List
import os
from novo_projeto.config import APP_KEY, APP_SECRET
from logger_config import obter_logger

# Configuração de logging
logger = obter_logger(__name__)


class BronzeProdutos:
    """Classe para coleta bronze de produtos do Omie"""

    def __init__(self):
        # Configurações da API Omie
        self.app_key = APP_KEY
        self.app_secret = APP_SECRET
        self.base_url = 'https://app.omie.com.br/api/v1/geral/produtos/'
        self.timeout = 60  # Aumentado timeout

        # Validação das credenciais
        if not self.app_key or not self.app_secret:
            raise ValueError(
                "APP_KEY e APP_SECRET devem estar configurados no config.py")

    def _make_request(self, data: Dict, max_retries: int = 3) -> Dict:
        """Faz requisição para a API do Omie com retry logic"""
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

    def coletar_produtos(self, pagina: int = 1, registros_por_pagina: int = 50) -> Dict:
        """Coleta todos os produtos do Omie sem filtro de data"""

        data = {
            "call": "ListarProdutos",
            "param": [{
                "pagina": pagina,
                "registros_por_pagina": registros_por_pagina,
                "apenas_importado_api": "N",
                "filtrar_apenas_omiepdv": "N",
                "filtrar_apenas_alteracao": "S"
            }]
        }

        logger.info(
            f"Coletando produtos - Página {pagina}, {registros_por_pagina} registros por página")
        return self._make_request(data)

    def processar_resposta(self, resposta: Dict) -> pd.DataFrame:
        """Processa a resposta da API e converte para DataFrame"""
        try:
            if 'produto_servico_cadastro' in resposta:
                produtos = resposta['produto_servico_cadastro']
                logger.info(f"Processando {len(produtos)} produtos")

                # Converte para DataFrame
                df = pd.DataFrame(produtos)

                # Adiciona metadados de coleta
                df['data_coleta'] = datetime.now()
                df['fonte'] = 'omie_api'
                df['camada'] = 'bronze'

                return df
            else:
                logger.warning("Nenhum produto encontrado na resposta")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Erro ao processar resposta: {e}")
            raise

    def salvar_parquet(self, df: pd.DataFrame, nome_arquivo: str = None) -> str:
        """Salva DataFrame como arquivo parquet"""
        if nome_arquivo is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo = f"bronze_produtos_{timestamp}.parquet"

        # Cria diretório se não existir
        os.makedirs(
            "maq_prod_estoque/bronze/produto_servico_fornecedor", exist_ok=True)
        caminho_arquivo = f"maq_prod_estoque/bronze/produto_servico_fornecedor/{nome_arquivo}"

        try:
            df.to_parquet(caminho_arquivo, index=False)
            logger.info(f"Arquivo salvo: {caminho_arquivo}")
            logger.info(f"Shape do DataFrame: {df.shape}")
            return caminho_arquivo
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo parquet: {e}")
            raise

    def coletar_todas_paginas(self, registros_por_pagina: int = 50, max_retries: int = 3) -> pd.DataFrame:
        """Coleta todos os produtos de todas as páginas, com retry em caso de erro, e retorna um DataFrame único"""
        pagina = 1
        todos_produtos = []
        paginas_puladas = []
        while True:
            tentativas = 0
            while tentativas < max_retries:
                try:
                    resposta = self.coletar_produtos(
                        pagina=pagina, registros_por_pagina=registros_por_pagina)
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
            if resposta and 'produto_servico_cadastro' in resposta:
                produtos = resposta['produto_servico_cadastro']
                if not produtos:
                    break
                todos_produtos.extend(produtos)
                total_paginas = resposta.get('total_de_paginas', 1)
                if pagina >= total_paginas:
                    break
                pagina += 1
            else:
                break
        if todos_produtos:
            df = pd.DataFrame(todos_produtos)
            df['data_coleta'] = datetime.now()
            df['fonte'] = 'omie_api'
            df['camada'] = 'bronze'
            if paginas_puladas:
                logger.warning(f"Páginas puladas: {paginas_puladas}")
            return df
        else:
            logger.warning(
                f"Nenhum produto coletado. Páginas puladas: {paginas_puladas}")
            return pd.DataFrame()


def main():
    """Função principal para execução e validação"""
    try:
        bronze = BronzeProdutos()
        logger.info("Iniciando coleta bronze de produtos (todas as páginas)...")
        df = bronze.coletar_todas_paginas(registros_por_pagina=50)
        if not df.empty:
            arquivo_parquet = bronze.salvar_parquet(
                df, "bronze_produtos.parquet")
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


if __name__ == "__main__":
    main()
