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

ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/produto_estoque/bronze_produto_estoque.parquet'

# Configuração de logging
logger = obter_logger(__name__)


class BronzeProdutos:
    """Classe para coleta bronze de produtos do Omie"""

    def __init__(self):
        # Configurações da API Omie
        self.app_key = APP_KEY
        self.app_secret = APP_SECRET
        self.base_url = 'https://app.omie.com.br/api/v1/estoque/consulta/'
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

    def coletar_posicao_estoque_paginado(self, data_posicao: str, pagina: int = 1, registros_por_pagina: int = 50) -> Dict:
        """Coleta posição de estoque de múltiplos produtos usando ListarPosEstoque (muito mais eficiente)"""

        data = {
            "call": "ListarPosEstoque",
            "param": [{
                "nPagina": pagina,
                "nRegPorPagina": registros_por_pagina,
                "dDataPosicao": data_posicao,
                "cExibeTodos": "S",  # Exibe todos os produtos mesmo sem movimento
                "codigo_local_estoque": 0
            }]
        }

        logger.info(
            f"Coletando posição de estoque - Data {data_posicao}, Página {pagina}, {registros_por_pagina} registros")
        return self._make_request(data)

    def coletar_produto_individual(self, data_posicao: str, codigo_produto: int) -> Dict:
        """Coleta posição de estoque de um produto específico (método original mantido para compatibilidade)"""

        data = {
            "call": "PosicaoEstoque",
            "param": [{
                "codigo_local_estoque": 0,
                "id_prod": codigo_produto,
                "data": data_posicao
            }]
        }

        logger.info(
            f"Coletando produto individual - Data {data_posicao}, ID {codigo_produto}")
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
        os.makedirs("maq_prod_estoque/bronze/produto_estoque", exist_ok=True)
        caminho_arquivo = f"maq_prod_estoque/bronze/produto_estoque/{nome_arquivo}"

        try:
            df.to_parquet(caminho_arquivo, index=False)
            logger.info(f"Arquivo salvo: {caminho_arquivo}")
            logger.info(f"Shape do DataFrame: {df.shape}")
            return caminho_arquivo
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo parquet: {e}")
            raise

    def coletar_todas_paginas(self, registros_por_pagina: int = 50, max_retries: int = 3, delay_segundos: float = 1.0) -> pd.DataFrame:
        """Coleta posição de estoque para todos os produtos usando ListarPosEstoque (muito mais eficiente)"""
        # Obtém a data atual no formato dd/mm/yyyy
        data_atual = datetime.now().strftime("%d/%m/%Y")

        logger.info(
            f"Iniciando coleta de posição de estoque na data {data_atual} usando ListarPosEstoque")

        pagina = 1
        todos_produtos = []
        paginas_com_erro = []
        max_paginas = 1000  # Contador de segurança para evitar loop infinito

        while pagina <= max_paginas:
            tentativas = 0
            resposta = None

            while tentativas < max_retries:
                try:
                    resposta = self.coletar_posicao_estoque_paginado(
                        data_posicao=data_atual,
                        pagina=pagina,
                        registros_por_pagina=registros_por_pagina
                    )
                    break  # Sucesso, sai do loop de tentativas

                except Exception as e:
                    tentativas += 1
                    logger.warning(
                        f"Página {pagina} (tentativa {tentativas}/{max_retries}): {e}")

                    if tentativas == max_retries:
                        logger.error(
                            f"Página {pagina} pulada após {max_retries} tentativas")
                        paginas_com_erro.append(pagina)
                        pagina += 1  # Tenta próxima página
                        resposta = None
                    else:
                        import time
                        time.sleep(2)  # Espera 2 segundos entre tentativas

            # Se não conseguiu resposta após todas as tentativas, para o loop
            if not resposta:
                logger.warning(
                    f"Não foi possível obter resposta para a página {pagina}")
                break

            # Processa a resposta se houver dados
            if 'produtos' in resposta:
                produtos = resposta['produtos']
                if produtos:
                    todos_produtos.extend(produtos)
                    logger.info(
                        f"Página {pagina}: {len(produtos)} produtos coletados")
                else:
                    logger.warning(f"Página {pagina}: Sem produtos")
                    break  # Não há mais produtos
            else:
                logger.warning(
                    f"Página {pagina}: Resposta vazia ou inválida")
                break  # Não há mais páginas

            # Verifica se há mais páginas
            total_paginas = resposta.get('nTotPaginas', 1)
            if pagina >= total_paginas:
                logger.info(
                    f"Última página atingida: {pagina}/{total_paginas}")
                break

            pagina += 1

            # Delay entre páginas para não sobrecarregar a API
            if delay_segundos > 0:
                import time
                time.sleep(delay_segundos)

        if todos_produtos:
            df = pd.DataFrame(todos_produtos)
            df['data_coleta'] = datetime.now()
            df['fonte'] = 'omie_api'
            df['camada'] = 'bronze'
            df['data_posicao_estoque'] = data_atual

            if paginas_com_erro:
                logger.warning(f"Páginas com erro: {paginas_com_erro}")

            logger.info(
                f"Coleta concluída: {len(todos_produtos)} registros coletados em {pagina-1} páginas")
            return df
        else:
            if pagina > max_paginas:
                logger.error(
                    f"Limite máximo de páginas atingido ({max_paginas}). Possível loop infinito.")
            logger.warning(
                f"Nenhum produto coletado. Páginas com erro: {paginas_com_erro}")
            return pd.DataFrame()

    def ler_codigos_produto(self, coluna_codigo: str = 'codigo_produto') -> pd.DataFrame:
        """Lê o arquivo Parquet ARQUIVO_BRONZE e retorna um DataFrame apenas com a coluna de código do produto."""
        try:
            df = pd.read_parquet(ARQUIVO_BRONZE)
            if coluna_codigo not in df.columns:
                logger.error(
                    f"Coluna '{coluna_codigo}' não encontrada no arquivo Parquet. Colunas disponíveis: {list(df.columns)}")
                raise ValueError(
                    f"Coluna '{coluna_codigo}' não encontrada no arquivo Parquet.")
            df_codigos = df[[coluna_codigo]].copy()
            df_codigos.rename(
                columns={coluna_codigo: 'codigo_produto'}, inplace=True)
            logger.info(
                f"DataFrame de códigos de produto criado com {len(df_codigos)} registros.")
            return df_codigos
        except Exception as e:
            logger.error(f"Erro ao ler códigos de produto do Parquet: {e}")
            raise


def main():
    """Função principal para execução e validação"""
    try:
        bronze = BronzeProdutos()
        logger.info(
            "Iniciando coleta bronze de posição de estoque usando ListarPosEstoque...")

        # Parâmetros otimizados
        df = bronze.coletar_todas_paginas(
            registros_por_pagina=100,  # Mais registros por página
            max_retries=3,
            delay_segundos=0.5  # Delay menor entre páginas
        )

        if not df.empty:
            arquivo_parquet = bronze.salvar_parquet(
                df, "bronze_produto_estoque.parquet")
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
