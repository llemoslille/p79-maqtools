# p79-maqtools

Repositório de **pipelines ETL** integrados à **API Omie**, com camadas estilo **medalhão** (bronze/raw → silver → gold), integração **Google Cloud Storage** e **BigQuery** onde aplicável, atualização de **dataset semântico** (Power BI via **MSAL**) e **telemetria opcional** de execução no **Supabase**.

Há **dois produtos ETL** independentes, cada um com seu próprio orquestrador na raiz do projeto:

| Pipeline        | Entrypoint               | Pasta de scripts   |
|----------------|--------------------------|--------------------|
| **Omie Vendas**   | `main_omie_vendas.py`    | `omie_vendas/`     |
| **Omie Estoque**  | `main_omie_estoque.py`   | `omie_estoque/`    |

---

## Requisitos

- **Python 3.10+** (recomendado 3.11 ou 3.12; use a mesma versão em desenvolvimento e no agendador).
- Conta/credenciais **Omie**, **GCP** (bucket e, quando necessário, BigQuery) e variáveis do **Power BI** onde o script `atualiza_dataset.py` for usado.

Instalação das dependências:

```powershell
cd C:\Repositorio\lille\p79-maqtools
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Dependências principais: `pandas`, `numpy`, `pyarrow`, `requests`, `google-cloud-storage`, `google-cloud-bigquery`, `google-auth`, `python-dotenv`, `PyYAML`, `msal`, `loguru`. Detalhes e versões fixadas em `requirements.txt`.

---

## Configuração

### Arquivo `.env` (raiz)

Carregado por `main_omie_vendas.py`, `main_omie_estoque.py` e por módulos que usam `python-dotenv`. **Não commite** o `.env` (está no `.gitignore`).

Variáveis usadas para **logs no Supabase** (todas opcionais; se faltarem URL/chave, o ETL roda normalmente sem telemetria):

| Variável | Descrição |
|----------|-----------|
| `SUPABASE_URL` | URL base do projeto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Chave service role |
| `SUPABASE_LOG_TABLE` | Tabela REST (padrão: `lille_atualizacoes_dw`) |
| `NODE_ENV` ou `ENV` | Ambiente lógico (ex.: `prod`) |
| `SUPABASE_APP` | Identificador da aplicação (padrão: `p79-maqtools`) |
| `SUPABASE_PROJECT_KEY` | Distingue vendas vs reposição (padrões nos mains: `p79-maqtools-vendas` e `p79-maqtools-reposicao`) |

**Power BI** (scripts `omie_vendas/atualiza_dataset.py` e `omie_estoque/atualiza_dataset.py`):

| Variável | Descrição |
|----------|-----------|
| `POWERBI_TENANT_ID` | Tenant Azure AD |
| `POWERBI_CLIENT_ID` | Application (client) ID |
| `POWERBI_CLIENT_SECRET` | Client secret (gerar no portal Azure; **não** versionar) |
| `POWERBI_GROUP_ID` | Workspace / group ID no Power BI |

**Google Cloud (service account JSON):**

- **Padrao do projeto:** usar o JSON na **raiz** do repositório: `machtools.json`.
- Defina no `.env`: `GOOGLE_APPLICATION_CREDENTIALS=machtools.json`.
- Opcionalmente, pode usar `GCS_CREDENTIALS_JSON_PATH` / `MACHTOOLS_JSON_PATH` para sobrescrever via ambiente.
- Guia completo de onde colocar as credenciais: `docs/ORIENTACAO_CREDENCIAIS_GCP.md`.

### `omie_estoque/novo_projeto/config.py`

Centraliza **APP_KEY / APP_SECRET** da Omie e parâmetros **GCS** (`GCS_BUCKET_NAME`, `GCS_PROJECT_ID`, `GCS_CREDENTIALS_PATH`) consumidos pelos scripts `nbronze_*`, `nsilver_*`, `ngold_*` e por `gcs_utils.py`. O JSON da service account **não** entra no Git (veja `.gitignore`).

### `omie_vendas/configuracoes/`

Parâmetros de API e caminhos (`configuracoes/variaveis.py`, `config.yaml` quando existir). **Revise credenciais** antes de publicar o repositório; prefira variáveis de ambiente ou arquivos ignorados pelo Git.

---

## Como executar

Sempre a partir da **raiz** do repositório (`C:\Repositorio\lille\p79-maqtools`), para que `load_dotenv` e imports encontrem `.env` e `supabase_events.py`.

### Omie Vendas

Pipeline completo (ordem das camadas definida em `main_omie_vendas.py`: `raw` → `omie` → `silver` → `gold` → `modelo_semantico`):

```powershell
.\.venv\Scripts\python.exe main_omie_vendas.py
```

Úteis:

```powershell
.\.venv\Scripts\python.exe main_omie_vendas.py --listar
.\.venv\Scripts\python.exe main_omie_vendas.py --camada silver
.\.venv\Scripts\python.exe main_omie_vendas.py --script raw_clientes.py
.\.venv\Scripts\python.exe main_omie_vendas.py --paralelo --workers 8
```

**Batch (venv):** `run_omie_vendas_venv.bat`  
**Batch (Python do sistema):** `run_omie_vendas.bat`

Os subprocessos rodam com `cwd` em `omie_vendas/`.

### Omie Estoque

Pipeline **Bronze → Silver → Gold** (scripts listados em `main_omie_estoque.py`):

```powershell
.\.venv\Scripts\python.exe main_omie_estoque.py
```

Úteis:

```powershell
.\.venv\Scripts\python.exe main_omie_estoque.py --silver-only
.\.venv\Scripts\python.exe main_omie_estoque.py --gold-only
.\.venv\Scripts\python.exe main_omie_estoque.py --script nbronze_pedido.py
```

**Batch (venv):** `run_omie_estoque_venv.bat`  
**Batch (Python do sistema):** `run_omie_estoque.bat`

Os subprocessos rodam com `cwd` em `omie_estoque/`.

### Dados locais (estoque)

O pipeline de estoque grava e lê **Parquet** sob `omie_estoque/maq_prod_estoque/` (camadas bronze/silver/gold). Essa árvore é **gerada em runtime** e está no `.gitignore`; o envio ao **GCS** complementa o fluxo conforme cada script.

---

## Estrutura resumida

```text
p79-maqtools/
├── main_omie_vendas.py      # Orquestrador vendas + Supabase opcional
├── main_omie_estoque.py     # Orquestrador estoque + Supabase opcional
├── supabase_events.py       # Cliente de eventos Supabase
├── requirements.txt
├── .env                     # Local; não versionar
├── omie_vendas/             # Scripts Omie, raw, silver, gold, atualiza_dataset
│   └── configuracoes/
├── omie_estoque/            # nbronze_*, nsilver_*, ngold_*, logger, gcs_utils
│   └── novo_projeto/        # config.py (Omie + GCS)
├── scripts/                 # Utilitários (ex.: consulta a logs Supabase)
├── extract_clientes.py      # Fluxo auxiliar de clientes (estrutura própria)
├── process_clientes.py
└── organizar_etl.py        # Organização de layout ETL (uso pontual)
```

---

## Segurança

- Mantenha **`.env`**, **JSON de service account** e **chaves Omie** fora do Git.
- Revise `omie_vendas/configuracoes/variaveis.py` e afins para não versionar segredos em claro.

### Push bloqueado no GitHub (“Push cannot contain secrets”)

O **GitHub Push Protection** recusa o `git push` se algum commit da enviada contiver credenciais. Além de **tirar os segredos dos arquivos**, é preciso que o histórico que você envia **não** inclua commits antigos com esses arquivos.

Se o remoto **ainda não** aceitou nenhum push com segredo, em geral basta **desfazer o commit local**, manter as alterações corrigidas e commitar de novo:

```powershell
cd C:\Repositorio\lille\p79-maqtools
git reset --soft HEAD~1
git add -A
git commit -m "Initial commit: ETL sem credenciais nos arquivos rastreados"
git push -u origin main
```

Confirme com `git log` que não há outro commit anterior contendo `machtools.json` ou segredos em `atualiza_dataset.py`. Se já existirem vários commits locais com segredo, use `git rebase -i` ou ferramentas como `git filter-repo` para limpar o histórico.

**Importante:** chaves que já apareceram em commit ou no GitHub devem ser consideradas **comprometidas**. **Revogue** o client secret no Azure, **crie nova chave** de service account no GCP e substitua os arquivos locais (`machtools.json`) e o `.env`.

### VS Code / Git lento (`git diff` em `.venv/`)

Garanta que **`.venv/`** está no `.gitignore` e que a pasta **nunca** foi adicionada com `git add .venv`. Se tiver ido parar no índice por engano: `git rm -r --cached .venv`.

---

## Documentação adicional

No repositório existem materiais complementares (ex.: `README_ETL.md`, notas de otimização e documentação sob `omie_estoque/`). Use-os como referência operacional; o fluxo oficial de automação descrito acima é via **`main_omie_vendas.py`** e **`main_omie_estoque.py`**.
