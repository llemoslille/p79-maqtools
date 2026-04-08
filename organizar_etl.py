"""
Script para organizar arquivos ETL em estrutura padronizada
Mantém os arquivos originais e cria cópias organizadas por camada
"""

import os
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

# Caminhos das pastas originais
PASTA_13_OMIE = Path("omie_estoque")
PASTA_P79_MACHTOOLS = Path("omie_vendas")

# Estrutura ETL de destino
ESTRUTURA_ETL = {
    "omie": [],           # Scripts de coleta da API Omie
    "bronze": [],         # Dados brutos (nbronze_*)
    "raw": [],            # Dados brutos alternativos (raw_*)
    "silver": [],         # Dados limpos (nsilver_*, silver_*)
    "gold": [],           # Dados consolidados (ngold_*, gold_*)
    "utils": [],          # Utilitários, configs, etc.
    "main": [],           # Orquestradores principais
    "docs": []            # Documentação
}

# Padrões de classificação
PADROES_CLASSIFICACAO = {
    "omie": [
        r"^Omie[_\.].*\.py$",
        r"^Omie\.py$",
        r"^omie[_\.].*\.py$",
        r"^omie\.py$",
    ],
    "bronze": [
        r"^nbronze_.*\.py$",
    ],
    "raw": [
        r"^raw_.*\.py$",
    ],
    "silver": [
        r"^nsilver_.*\.py$",
        r"^silver_.*\.py$",
    ],
    "gold": [
        r"^ngold_.*\.py$",
        r"^gold_.*\.py$",
    ],
    "main": [
        r"^main.*\.py$",
    ],
    "utils": [
        r"^config.*\.py$",
        r"^.*_config\.py$",
        r"^logger_config\.py$",
        r"^gcs_utils\.py$",
        r"^atualiza_dataset\.py$",
        r"^.*\.json$",
        r"^.*\.yaml$",
        r"^.*\.yml$",
        r"^requirements\.txt$",
    ],
    "docs": [
        r"^.*\.md$",
        r"^.*README.*$",
    ]
}

# Arquivos a ignorar
ARQUIVOS_IGNORAR = [
    "__pycache__",
    ".git",
    ".gitignore",
    "*.pyc",
    "*.pkl",
    "*.parquet",
    "*.txt",
    "*.log",
    "*.bat",
    "temp_*",
    "resultado_*",
    "check_*",
    "verificar_*",
    "buscar_*",
    "investigar_*",
    "teste_*",
    "enviar_email_*",
    "retry_*",
    "produtos_cache.pkl",
    "produtos_com_erro.txt",
]


def classificar_arquivo(nome_arquivo: str) -> str:
    """
    Classifica um arquivo em uma das camadas ETL baseado no nome
    
    Args:
        nome_arquivo: Nome do arquivo
        
    Returns:
        Nome da camada ou 'utils' se não corresponder a nenhum padrão
    """
    import re
    
    for camada, padroes in PADROES_CLASSIFICACAO.items():
        for padrao in padroes:
            if re.match(padrao, nome_arquivo, re.IGNORECASE):
                return camada
    
    # Se não corresponde a nenhum padrão específico, vai para utils
    return "utils"


def deve_ignorar_arquivo(nome_arquivo: str) -> bool:
    """
    Verifica se um arquivo deve ser ignorado
    
    Args:
        nome_arquivo: Nome do arquivo
        
    Returns:
        True se deve ignorar, False caso contrário
    """
    import fnmatch
    
    for padrao in ARQUIVOS_IGNORAR:
        if fnmatch.fnmatch(nome_arquivo, padrao):
            return True
        if padrao in nome_arquivo:
            return True
    
    return False


def criar_estrutura_diretorios(base_path: Path):
    """
    Cria a estrutura de diretórios ETL
    
    Args:
        base_path: Caminho base onde criar a estrutura
    """
    for camada in ESTRUTURA_ETL.keys():
        dir_path = base_path / "etl" / camada
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"[OK] Diretorio criado: {dir_path}")


def copiar_arquivo(origem: Path, destino: Path, camada: str):
    """
    Copia um arquivo para a estrutura ETL
    
    Args:
        origem: Caminho do arquivo original
        destino: Caminho de destino na estrutura ETL
        camada: Nome da camada
    """
    try:
        # Criar diretório de destino se não existir
        destino.parent.mkdir(parents=True, exist_ok=True)
        
        # Copiar arquivo
        shutil.copy2(origem, destino)
        print(f"  [OK] Copiado: {origem.name} -> etl/{camada}/")
        return True
    except Exception as e:
        print(f"  [ERRO] Erro ao copiar {origem.name}: {e}")
        return False


def processar_pasta(pasta_origem: Path, pasta_base_etl: Path, nome_projeto: str):
    """
    Processa uma pasta e organiza os arquivos na estrutura ETL
    
    Args:
        pasta_origem: Pasta original com os arquivos
        pasta_base_etl: Pasta base onde criar a estrutura ETL
        nome_projeto: Nome do projeto (para subpasta)
    """
    if not pasta_origem.exists():
        print(f"[AVISO] Pasta nao encontrada: {pasta_origem}")
        return
    
    print(f"\n{'='*80}")
    print(f"Processando: {pasta_origem}")
    print(f"{'='*80}")
    
    arquivos_processados = {camada: 0 for camada in ESTRUTURA_ETL.keys()}
    
    # Processar arquivos Python
    for arquivo in pasta_origem.rglob("*.py"):
        # Pular __pycache__ e outros diretórios especiais
        if "__pycache__" in str(arquivo) or deve_ignorar_arquivo(arquivo.name):
            continue
        
        # Classificar arquivo
        camada = classificar_arquivo(arquivo.name)
        
        # Caminho de destino
        destino = pasta_base_etl / "etl" / camada / nome_projeto / arquivo.name
        
        # Copiar arquivo
        if copiar_arquivo(arquivo, destino, camada):
            arquivos_processados[camada] += 1
    
    # Processar arquivos de configuração e documentação
    extensoes_extras = [".json", ".yaml", ".yml", ".md", ".txt"]
    for ext in extensoes_extras:
        for arquivo in pasta_origem.rglob(f"*{ext}"):
            if deve_ignorar_arquivo(arquivo.name):
                continue
            
            # Classificar arquivo
            camada = classificar_arquivo(arquivo.name)
            
            # Caminho de destino
            destino = pasta_base_etl / "etl" / camada / nome_projeto / arquivo.name
            
            # Copiar arquivo
            if copiar_arquivo(arquivo, destino, camada):
                arquivos_processados[camada] += 1
    
    # Resumo
    print(f"\n{'='*80}")
    print(f"Resumo - {nome_projeto}:")
    print(f"{'='*80}")
    total = 0
    for camada, quantidade in arquivos_processados.items():
        if quantidade > 0:
            print(f"  {camada.upper():10s}: {quantidade:3d} arquivo(s)")
            total += quantidade
    print(f"  {'TOTAL':10s}: {total:3d} arquivo(s)")
    print(f"{'='*80}\n")


def main():
    """
    Função principal que organiza todos os arquivos ETL
    """
    print("="*80)
    print("ORGANIZADOR DE ETL - Estrutura Medalhão")
    print("="*80)
    print("\nEste script irá:")
    print("  1. Criar estrutura de diretórios ETL")
    print("  2. Copiar arquivos das pastas originais para a nova estrutura")
    print("  3. Manter os arquivos originais intactos")
    print("\n" + "="*80 + "\n")
    
    # Caminho base (raiz do projeto)
    pasta_base = Path.cwd()
    pasta_etl = pasta_base / "etl"
    
    # Criar estrutura de diretórios
    print("Criando estrutura de diretórios...")
    criar_estrutura_diretorios(pasta_base)
    
    # Processar pasta omie_estoque
    processar_pasta(
        PASTA_13_OMIE,
        pasta_base,
        "omie_estoque"
    )
    
    # Processar pasta omie_vendas
    processar_pasta(
        PASTA_P79_MACHTOOLS,
        pasta_base,
        "omie_vendas"
    )
    
    # Criar arquivo README na estrutura ETL
    criar_readme_etl(pasta_etl)
    
    print("\n" + "="*80)
    print("[OK] ORGANIZACAO CONCLUIDA COM SUCESSO!")
    print("="*80)
    print(f"\nEstrutura criada em: {pasta_etl}")
    print("\nCamadas disponíveis:")
    for camada in ESTRUTURA_ETL.keys():
        camada_path = pasta_etl / camada
        if camada_path.exists():
            subdirs = [d.name for d in camada_path.iterdir() if d.is_dir()]
            if subdirs:
                print(f"  - {camada}/ ({len(subdirs)} projeto(s))")
            else:
                print(f"  - {camada}/ (vazia)")
    print("\n" + "="*80)


def criar_readme_etl(pasta_etl: Path):
    """
    Cria um README explicando a estrutura ETL
    
    Args:
        pasta_etl: Caminho da pasta ETL
    """
    readme_content = """# Estrutura ETL - Arquitetura Medalhão

Esta estrutura organiza os scripts ETL seguindo a arquitetura medalhão (Medallion Architecture).

## 📁 Estrutura de Camadas

### 🔵 OMIE
Scripts que coletam dados diretamente da API Omie.
- `Omie_*.py` - Scripts de coleta da API

### 🥉 BRONZE
Camada de dados brutos, sem transformação. Primeira camada após a coleta.
- `nbronze_*.py` - Scripts que coletam dados brutos da API

### 📦 RAW
Camada alternativa de dados brutos (usada no projeto omie_vendas).
- `raw_*.py` - Scripts de dados brutos alternativos

### 🥈 SILVER
Camada de dados limpos e normalizados. Processa dados do Bronze/RAW.
- `nsilver_*.py` - Scripts de processamento Silver
- `silver_*.py` - Scripts Silver alternativos

### 🥇 GOLD
Camada de dados consolidados e prontos para análise. Processa dados do Silver.
- `ngold_*.py` - Scripts de consolidação Gold
- `gold_*.py` - Scripts Gold alternativos

### 🛠️ UTILS
Arquivos utilitários, configurações e helpers.
- `config_*.py` - Arquivos de configuração
- `logger_config.py` - Configuração de logging
- `gcs_utils.py` - Utilitários do Google Cloud Storage
- `*.json`, `*.yaml` - Arquivos de configuração
- `requirements.txt` - Dependências

### 🎯 MAIN
Orquestradores principais que executam o pipeline completo.
- `main.py` - Script principal de orquestração

### 📚 DOCS
Documentação do projeto.
- `*.md` - Arquivos Markdown de documentação

## 🔄 Fluxo de Dados

```
OMIE → BRONZE/RAW → SILVER → GOLD → Modelo Semântico
```

1. **OMIE**: Coleta dados da API Omie
2. **BRONZE/RAW**: Armazena dados brutos sem transformação
3. **SILVER**: Limpa, normaliza e enriquece os dados
4. **GOLD**: Consolida e valida os dados para análise
5. **Modelo Semântico**: Atualiza datasets no BigQuery

## 📂 Organização por Projeto

Cada camada contém subpastas por projeto:
- `omie_estoque/` - Scripts do projeto omie_estoque
- `omie_vendas/` - Scripts do projeto omie_vendas

## ⚠️ Importante

- **Os arquivos originais são mantidos intactos** nas pastas originais
- Esta estrutura é uma **cópia organizada** dos arquivos
- Para executar os scripts, use os arquivos originais ou ajuste os imports

## 🚀 Como Usar

1. Execute `python organizar_etl.py` para criar/atualizar a estrutura
2. Navegue pelas camadas para encontrar os scripts organizados
3. Use os scripts originais para execução (mantêm os imports corretos)

## 📝 Notas

- Arquivos temporários, logs e arquivos de teste são ignorados
- A estrutura é recriada a cada execução do script organizador
- Arquivos duplicados são sobrescritos
"""
    
    readme_path = pasta_etl / "README.md"
    readme_path.write_text(readme_content, encoding="utf-8")
    print(f"\n[OK] README criado: {readme_path}")


if __name__ == "__main__":
    main()
