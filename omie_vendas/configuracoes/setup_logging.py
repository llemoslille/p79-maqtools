"""
Configuração centralizada de logging para o pipeline P79 Machtools
Mantém apenas 2 arquivos de log: o atual e o anterior
"""
import os
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime


def configurar_logging(nome_script):
    """
    Configura o sistema de logging para manter apenas 2 arquivos:
    - Log atual (data de hoje)
    - Log anterior (última execução)
    
    Args:
        nome_script (str): Nome do script que está sendo executado
    """
    # Remove configuração padrão do loguru
    logger.remove()
    
    # Adiciona saída para console com formatação colorida
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{extra[script]}</cyan> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # Define pasta de logs
    pasta_logs = Path(__file__).parent.parent / "logs"
    pasta_logs.mkdir(exist_ok=True)
    
    # Nome do arquivo de log com data atual
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    arquivo_log = pasta_logs / f"pipeline_{data_hoje}.log"
    
    # Gerenciar rotação manual - manter apenas 2 arquivos
    gerenciar_logs_antigos(pasta_logs, arquivo_log)
    
    # Adiciona saída para arquivo
    logger.add(
        str(arquivo_log),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[script]} | {message}",
        level="DEBUG",
        rotation="00:00",  # Rotaciona à meia-noite
        retention=1,  # Mantém apenas 1 arquivo antigo (além do atual)
        compression=None,  # Sem compressão para facilitar leitura
        encoding="utf-8"
    )
    
    # Adiciona contexto do script
    logger.configure(extra={"script": nome_script})
    
    return logger


def gerenciar_logs_antigos(pasta_logs, arquivo_atual):
    """
    Mantém apenas 2 arquivos de log: o atual e o mais recente anterior
    Remove todos os outros arquivos antigos
    
    Args:
        pasta_logs (Path): Caminho da pasta de logs
        arquivo_atual (Path): Caminho do arquivo de log atual
    """
    try:
        # Lista todos os arquivos de log
        arquivos_log = sorted(
            pasta_logs.glob("pipeline_*.log"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        # Remove o arquivo atual da lista se existir
        arquivos_log = [f for f in arquivos_log if f != arquivo_atual]
        
        # Mantém apenas o mais recente (além do atual)
        # Remove todos os outros
        if len(arquivos_log) > 1:
            for arquivo_antigo in arquivos_log[1:]:
                try:
                    arquivo_antigo.unlink()
                    print(f"[LOG] Arquivo antigo removido: {arquivo_antigo.name}")
                except Exception as e:
                    print(f"[LOG] Erro ao remover {arquivo_antigo.name}: {e}")
                    
    except Exception as e:
        print(f"[LOG] Erro ao gerenciar logs antigos: {e}")


def get_logger(nome_script):
    """
    Retorna o logger configurado para o script
    
    Args:
        nome_script (str): Nome do script (ex: "Omie.py", "raw_pedido_vendas.py")
    
    Returns:
        logger: Instância do logger configurado
    """
    return configurar_logging(nome_script)


# Função auxiliar para obter caminho do log atual
def get_log_atual():
    """
    Retorna o caminho do arquivo de log atual
    
    Returns:
        str: Caminho completo do arquivo de log atual
    """
    pasta_logs = Path(__file__).parent.parent / "logs"
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    arquivo_log = pasta_logs / f"pipeline_{data_hoje}.log"
    return str(arquivo_log)


if __name__ == "__main__":
    # Teste da configuração
    test_logger = get_logger("TESTE")
    test_logger.info("Sistema de logging configurado com sucesso!")
    test_logger.debug("Log de debug")
    test_logger.warning("Log de aviso")
    test_logger.error("Log de erro")
    test_logger.success("Log de sucesso")
    print(f"\nArquivo de log: {get_log_atual()}")



