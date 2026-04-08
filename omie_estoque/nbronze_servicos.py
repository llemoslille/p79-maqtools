#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bronze Layer - Coleta inicial de dados de serviços do Omie
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


class BronzeServicos:
    """Classe para coleta bronze de serviços do Omie"""

    def __init__(self):
        # Configurações da API Omie
        self.app_key = APP_KEY
        self.app_secret = APP_SECRET
        self.base_url = 'https://app.omie.com.br/api/v1/servicos/servico/'
        self.timeout = 30

        # Validação das credenciais
        if not self.app_key or not self.app_secret:
            raise ValueError(
                "APP_KEY e APP_SECRET devem estar configurados no config.py")

    def _make_request(self, data: Dict) -> Dict:
        """Faz requisição para a API do Omie"""
        payload = {
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            **data
        }

        try:
            logger.info(f"Fazendo requisição para: {self.base_url}")
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            response = requests.post(
                self.base_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            resposta = response.json()
            logger.info(f"Resposta da API: {json.dumps(resposta, indent=2)}")
            return resposta
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            raise

    def coletar_servicos(self, pagina: int = 1, registros_por_pagina: int = 20) -> Dict:
        """Coleta todos os serviços do Omie"""

        data = {
            "call": "ListarCadastroServico",
            "param": [{
                "nPagina": pagina,
                "nRegPorPagina": registros_por_pagina
            }]
        }

        logger.info(
            f"Coletando serviços - Página {pagina}, {registros_por_pagina} registros por página")
        return self._make_request(data)

    def processar_resposta(self, resposta: Dict) -> pd.DataFrame:
        """Processa a resposta da API e converte para DataFrame"""
        try:
            if 'cadastros' in resposta:
                cadastros = resposta['cadastros']
                logger.info(f"Processando {len(cadastros)} serviços")
                # Converte para DataFrame
                df = pd.DataFrame(cadastros)
                # Adiciona metadados de coleta
                df['data_coleta'] = datetime.now()
                df['fonte'] = 'omie_api'
                df['camada'] = 'bronze'
                return df
            else:
                logger.warning(
                    "Nenhum serviço encontrado na resposta (campo 'cadastros' ausente)")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro ao processar resposta: {e}")
            raise

    def salvar_parquet(self, df: pd.DataFrame, nome_arquivo: str = None) -> str:
        """Salva DataFrame como arquivo parquet"""
        if nome_arquivo is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo = f"bronze_servicos_{timestamp}.parquet"

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

    def coletar_todas_paginas(self, registros_por_pagina: int = 20, max_retries: int = 3) -> pd.DataFrame:
        """Coleta todos os serviços de todas as páginas, com retry em caso de erro, e retorna um DataFrame único"""
        pagina = 1
        todos_servicos = []
        paginas_puladas = []

        while True:
            tentativas = 0
            while tentativas < max_retries:
                try:
                    resposta = self.coletar_servicos(
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

            if resposta and 'cadastros' in resposta:
                cadastros = resposta['cadastros']
                if not cadastros:
                    break
                todos_servicos.extend(cadastros)
                # Verifica se há mais páginas
                nTotPaginas = resposta.get('nTotPaginas', 1)
                if pagina >= nTotPaginas:
                    break
                pagina += 1
            else:
                break

        if todos_servicos:
            df = pd.DataFrame(todos_servicos)
            df['data_coleta'] = datetime.now()
            df['fonte'] = 'omie_api'
            df['camada'] = 'bronze'

            if paginas_puladas:
                logger.warning(f"Páginas puladas: {paginas_puladas}")
            return df
        else:
            logger.warning(
                f"Nenhum serviço coletado. Páginas puladas: {paginas_puladas}")
            return pd.DataFrame()


def main():
    """Função principal para execução e validação"""
    try:
        bronze = BronzeServicos()
        logger.info("Iniciando coleta bronze de serviços (todas as páginas)...")
        df = bronze.coletar_todas_paginas(registros_por_pagina=20)

        if not df.empty:
            arquivo_parquet = bronze.salvar_parquet(
                df, "bronze_servicos.parquet")
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
