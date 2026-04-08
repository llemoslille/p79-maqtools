from __future__ import annotations

from pathlib import Path

### URL OMIE
omie_v1 = "https://app.omie.com.br/api/v1"

# Clientes (API Geral)
# Mantém compatibilidade com scripts `raw_clientes.py` / `silver_clientes.py`
base_url = "https://app.omie.com.br/api/v1"
version = "/geral"
endpoint_clientes = "/clientes/"

### ENDPOINTS
nfse = "/servicos/nfse/"
ordem_servico = "/servicos/os/"

### CHAMADAS - CALL
lista_nfse = "ListarNFSEs"
lista_os = "ListarOS"


## PASTAS E ARQUIVOS
#
# Estes caminhos eram hardcoded para um ambiente antigo (C:\\Repositorio\\Python\\...).
# Para permitir execução em qualquer máquina/pasta, calculamos relativo a este arquivo.
_CONFIG_DIR = Path(__file__).resolve().parent
pasta_root = str(_CONFIG_DIR.parent)  # .../omie_vendas
arquivo_yaml = str(_CONFIG_DIR / "config.yaml")


# Credenciais Omie
app_key = "1733209266789"
app_secret = "14c2f271c839dfea25cfff4afb63b331"