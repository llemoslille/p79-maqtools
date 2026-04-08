"""
Sistema de Logging Centralizado - ETL Omie
==========================================

Este módulo configura o sistema de logging para todo o projeto.
Mantém apenas os 2 últimos arquivos de log (anterior e atual).

Características:
- Logs salvos em: logs/etl_YYYYMMDD_HHMMSS.log
- Mantém apenas 2 arquivos (último e atual)
- Logs no console E em arquivo simultaneamente
- Formato padronizado com timestamp, nível e mensagem
"""

import logging
import os
from datetime import datetime
from pathlib import Path
import glob


class ETLLogger:
    """Gerenciador de logs do ETL com rotação automática"""
    
    PASTA_LOGS = "logs"
    PREFIXO_LOG = "etl_"
    MAX_ARQUIVOS = 2  # Mantém apenas 2 arquivos (anterior e atual)
    
    def __init__(self, nome_modulo: str = None):
        """
        Inicializa o logger
        
        Args:
            nome_modulo: Nome do módulo/script que está usando o logger
        """
        self.nome_modulo = nome_modulo or __name__
        self.logger = None
        self.arquivo_log_atual = None
        
    def configurar(self, nivel=logging.INFO) -> logging.Logger:
        """
        Configura o sistema de logging
        
        Args:
            nivel: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            
        Returns:
            Logger configurado
        """
        # Criar pasta de logs se não existir
        os.makedirs(self.PASTA_LOGS, exist_ok=True)
        
        # Limpar logs antigos (manter apenas os 2 mais recentes)
        self._limpar_logs_antigos()
        
        # Criar nome do arquivo de log com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.arquivo_log_atual = os.path.join(
            self.PASTA_LOGS, 
            f"{self.PREFIXO_LOG}{timestamp}.log"
        )
        
        # Configurar logger
        self.logger = logging.getLogger(self.nome_modulo)
        self.logger.setLevel(nivel)
        
        # Remover handlers existentes para evitar duplicação
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Formato do log
        formato = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo
        file_handler = logging.FileHandler(
            self.arquivo_log_atual, 
            mode='a', 
            encoding='utf-8'
        )
        file_handler.setLevel(nivel)
        file_handler.setFormatter(formato)
        self.logger.addHandler(file_handler)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(nivel)
        console_handler.setFormatter(formato)
        self.logger.addHandler(console_handler)
        
        # Log inicial
        self.logger.info("=" * 80)
        self.logger.info(f"Sistema de Logging Iniciado - Módulo: {self.nome_modulo}")
        self.logger.info(f"Arquivo de log: {self.arquivo_log_atual}")
        self.logger.info("=" * 80)
        
        return self.logger
    
    def _limpar_logs_antigos(self):
        """Remove logs antigos, mantendo apenas os MAX_ARQUIVOS mais recentes"""
        try:
            # Buscar todos os arquivos de log
            padrao = os.path.join(self.PASTA_LOGS, f"{self.PREFIXO_LOG}*.log")
            arquivos_log = glob.glob(padrao)
            
            # Ordenar por data de modificação (mais recente primeiro)
            arquivos_log.sort(key=os.path.getmtime, reverse=True)
            
            # Manter apenas os MAX_ARQUIVOS - 1 (para dar espaço ao novo)
            arquivos_para_remover = arquivos_log[self.MAX_ARQUIVOS - 1:]
            
            for arquivo in arquivos_para_remover:
                try:
                    os.remove(arquivo)
                    print(f"[LOG] Arquivo antigo removido: {arquivo}")
                except Exception as e:
                    print(f"[AVISO] Não foi possível remover {arquivo}: {e}")
                    
        except Exception as e:
            print(f"[AVISO] Erro ao limpar logs antigos: {e}")
    
    def obter_caminho_log_atual(self) -> str:
        """Retorna o caminho do arquivo de log atual"""
        return self.arquivo_log_atual
    
    def listar_logs_existentes(self) -> list:
        """Lista todos os arquivos de log existentes"""
        padrao = os.path.join(self.PASTA_LOGS, f"{self.PREFIXO_LOG}*.log")
        arquivos = glob.glob(padrao)
        arquivos.sort(key=os.path.getmtime, reverse=True)
        return arquivos


def obter_logger(nome_modulo: str = None, nivel=logging.INFO) -> logging.Logger:
    """
    Função auxiliar para obter um logger configurado
    
    Args:
        nome_modulo: Nome do módulo/script
        nivel: Nível de log
        
    Returns:
        Logger configurado
        
    Exemplo:
        logger = obter_logger(__name__)
        logger.info("Mensagem de log")
    """
    etl_logger = ETLLogger(nome_modulo)
    return etl_logger.configurar(nivel)


# ============================================================================
# EXEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    # Teste do sistema de logging
    logger = obter_logger("teste_logging")
    
    logger.debug("Mensagem de DEBUG")
    logger.info("Mensagem de INFO")
    logger.warning("Mensagem de WARNING")
    logger.error("Mensagem de ERROR")
    logger.critical("Mensagem de CRITICAL")
    
    # Listar logs existentes
    etl = ETLLogger()
    logs = etl.listar_logs_existentes()
    print(f"\n[INFO] Logs existentes: {len(logs)}")
    for log in logs:
        print(f"  - {log}")

