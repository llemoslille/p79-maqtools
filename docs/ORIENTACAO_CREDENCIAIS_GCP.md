# Credenciais GCP (JSON)

Este guia define o padrao de credenciais do projeto para evitar erro de execucao e vazamento de segredo no Git.

## Padrao oficial

- Arquivo da service account: `machtools.json`
- Local: **raiz do repositorio** (`machtools.json`)
- O arquivo **nao deve** ser commitado.

## Configuracao no `.env`

Use preferencialmente:

```env
GOOGLE_APPLICATION_CREDENTIALS=machtools.json
```

Tambem sao aceitas (casos especiais):

- `GCS_CREDENTIALS_JSON_PATH`
- `MACHTOOLS_JSON_PATH`

## Como os pipelines leem a credencial

### `omie_vendas`

- Usa o caminho da raiz padronizado.
- Se `GOOGLE_APPLICATION_CREDENTIALS` estiver definido, ele prevalece para os scripts que usam ambiente.

### `omie_estoque` (`omie_reposicao`)

Arquivo: `omie_estoque/novo_projeto/config.py`

Ordem de resolucao:

1. `GCS_CREDENTIALS_JSON_PATH`
2. `MACHTOOLS_JSON_PATH`
3. `GOOGLE_APPLICATION_CREDENTIALS`
4. `machtools.json` na raiz

## Checklist rapido (antes do push)

- Verificar rastreamento de arquivos sensiveis:
  - `git status --short`
- Se algum JSON entrou no indice:
  - `git rm --cached <caminho-do-json>`
- Confirmar que `.gitignore` cobre arquivos de credenciais.

## Troubleshooting

- **Erro "credenciais nao encontradas"**:
  - conferir se `machtools.json` existe na raiz
  - conferir valor de `GOOGLE_APPLICATION_CREDENTIALS` no `.env`
- **Push bloqueado por segredo**:
  - remover o segredo dos arquivos
  - limpar historico local que contenha a credencial
  - rotacionar a chave no GCP se ela ja foi exposta
