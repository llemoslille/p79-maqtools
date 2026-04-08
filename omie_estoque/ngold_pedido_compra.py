import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _bootstrap_env() -> None:
    """Carrega .env da raiz do repo e mapeia GOOGLE_APPLICATION_CREDENTIALS para GCS."""
    try:
        from dotenv import load_dotenv

        load_dotenv(_REPO_ROOT / ".env")
    except ImportError:
        pass
    gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if gac and not os.environ.get("GCS_CREDENTIALS_JSON_PATH") and not os.environ.get(
        "MACHTOOLS_JSON_PATH"
    ):
        p = Path(gac)
        if not p.is_absolute():
            p = _REPO_ROOT / p
        if p.is_file():
            os.environ["GCS_CREDENTIALS_JSON_PATH"] = str(p.resolve())


_bootstrap_env()

import pandas as pd

# Quando o Silver concatena histórico + staging, itens removidos na OMIE podem permanecer
# apenas com datetime de coleta antigo. Este filtro remove essas linhas por pedido.
# Desative com GOLD_PEDIDO_COMPRA_DESATIVAR_FILTRO_COLETA=1 no ambiente.
FILTRAR_ITENS_COLETA_ANTIGA_POR_PEDIDO = (
    os.environ.get("GOLD_PEDIDO_COMPRA_DESATIVAR_FILTRO_COLETA", "")
    .strip()
    .lower()
    not in ("1", "true", "yes", "sim")
)

# Usar diretamente o Silver Final como fonte de dados
ARQUIVO_SILVER_GCS = 'silver/pedidos_compras/pedidos_compras.parquet'
ARQUIVO_SILVER_LOCAL = 'temp_pedidos_compras_final.parquet'
# Caminho absoluto ou relativo à raiz do repo para pular o download do GCS.
SILVER_PARQUET_OVERRIDE = os.environ.get("GOLD_PEDIDO_COMPRA_SILVER_PARQUET", "").strip()
SKIP_UPLOAD_GCS = os.environ.get("GOLD_PEDIDO_COMPRA_SKIP_UPLOAD", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "sim",
)
ARQUIVO_GOLD = 'maq_prod_estoque/gold/pedidos/gold_pedido_compra_fato.parquet'
os.makedirs('maq_prod_estoque/gold/pedidos', exist_ok=True)

# Configurações do GCS
GCS_PATH = "gold/pedidos/gold_pedido_compra_fato.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py

def baixar_silver_final_gcs():
    """Baixa o arquivo Silver Final do GCS"""
    from google.cloud import storage
    from novo_projeto.config import GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH, GCS_PROJECT_ID

    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(ARQUIVO_SILVER_GCS)
        blob.download_to_filename(ARQUIVO_SILVER_LOCAL)
        print(
            f"[GCS] Silver Final baixado: {ARQUIVO_SILVER_GCS} -> {ARQUIVO_SILVER_LOCAL}")
        return True
    except Exception as e:
        print(f"[ERRO] Erro ao baixar Silver Final do GCS: {e}")
        return False


def _filtrar_itens_coleta_antiga_por_pedido(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Para cada codigo_pedido, se existir mais de um instante distinto de coleta (entre as linhas
    com data válida), mantém somente as linhas do instante mais recente.

    Assim remove itens 'fantasma' que ficaram só no arquivo histórico quando o pedido foi
    recarregado sem esse item no snapshot atual.
    """
    if df is None or len(df) == 0 or "codigo_pedido" not in df.columns:
        return df, 0

    colunas_data = [
        "datetime_coleta_dados",
        "datetime_processamento",
        "data_processamento_final",
        "data_coleta",
        "data_arquivo",
    ]
    col_data = next((c for c in colunas_data if c in df.columns), None)
    if not col_data:
        print(
            "[INFO] Filtro coleta por pedido: nenhuma coluna de data conhecida — etapa ignorada."
        )
        return df, 0

    work = df.copy()
    serie = work[col_data]
    if col_data == "datetime_coleta_dados":
        ts = pd.to_datetime(serie, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    else:
        ts = pd.to_datetime(serie, errors="coerce")
    work["_ts_coleta"] = ts

    ped = work["codigo_pedido"].astype(str)
    # Quantidade de timestamps distintos (NaT não entra no nunique) por pedido
    nunique_ts = work.groupby(ped, sort=False)["_ts_coleta"].transform("nunique")
    ts_max = work.groupby(ped, sort=False)["_ts_coleta"].transform("max")

    precisa_filtrar = nunique_ts > 1
    manter = ~precisa_filtrar | (
        work["_ts_coleta"].notna() & (work["_ts_coleta"] == ts_max)
    )

    removidos = int((~manter).sum())
    if removidos:
        print(
            f"[INFO] Filtro coleta por pedido ({col_data}): removidas {removidos} linhas "
            f"com snapshot mais antigo que o mais recente do mesmo codigo_pedido."
        )
    else:
        print(
            f"[INFO] Filtro coleta por pedido ({col_data}): nenhuma linha removida."
        )

    out = work.loc[manter].drop(columns=["_ts_coleta"])
    return out, removidos


COLUNAS_GOLD = [
    'cod_etapa',
    'nr_pedido',
    'dt_previsao_entrega',
    'dt_pedido',
    'cod_fornecedor',
    'codigo_pedido',
    'cod_produto',
    'un_produto',
    'cod_item_pedido_compra',
    'produto_id',
    'vlr_desconto',
    'vlr_despesa',
    'vlr_frete',
    'qtd',
    'qtd_recebida',
    'vlr_seguro',
    'vlr_total_produto_pedido',
    'vlr_total_item',
    'vlr_unitario_pedido',
    'vlr_cofins',
    'vlr_icms',
    'vlr_ipi',
    'vlr_pis',
    'vlr_icms_st',
    'dt_vencimento',
    'nr_dias',
    'nr_parcela',
    'cod_frete_modalidade',
    'fl_encerrado',
]


def main():
    print("[GOLD FINAL] Gerando camada Gold a partir do Silver Final...")

    caminho_silver: str | None = None
    if SILVER_PARQUET_OVERRIDE:
        cand = Path(SILVER_PARQUET_OVERRIDE)
        if not cand.is_file():
            cand = _REPO_ROOT / SILVER_PARQUET_OVERRIDE
        if cand.is_file():
            caminho_silver = str(cand.resolve())
            print(f"\n[PASSO 1] Silver local (GOLD_PEDIDO_COMPRA_SILVER_PARQUET): {caminho_silver}")
        else:
            print(
                f"[ERRO] GOLD_PEDIDO_COMPRA_SILVER_PARQUET não encontrado: {SILVER_PARQUET_OVERRIDE}"
            )
            return
    else:
        print("\n[PASSO 1] Baixando Silver Final do GCS...")
        if not baixar_silver_final_gcs():
            print("[ERRO] Não foi possível baixar o Silver Final. Abortando...")
            print(
                "[INFO] Dica: baixe pedidos_compras.parquet do GCS e defina "
                "GOLD_PEDIDO_COMPRA_SILVER_PARQUET=caminho\\para\\pedidos_compras.parquet"
            )
            return

    # 2. Carregar Silver Final
    print("\n[PASSO 2] Carregando Silver Final...")
    try:
        df = pd.read_parquet(caminho_silver or ARQUIVO_SILVER_LOCAL)
        print(f"[INFO] Silver Final carregado: {df.shape}")
        print(f"[INFO] Colunas disponíveis: {len(df.columns)} colunas")

        # Mostra estatísticas do Silver Final
        if 'origem_arquivo' in df.columns:
            print(
                f"[INFO] Origens dos dados: {df['origem_arquivo'].value_counts().to_dict()}")
        if 'codigo_pedido' in df.columns:
            print(f"[INFO] Pedidos únicos: {df['codigo_pedido'].nunique()}")
        if 'produto_id' in df.columns:
            print(f"[INFO] Produtos únicos: {df['produto_id'].nunique()}")

    except Exception as e:
        print(f"[ERRO] Erro ao carregar Silver Final: {e}")
        return

    # 2.1 Remover itens defasados por pedido (histórico x snapshot atual)
    if FILTRAR_ITENS_COLETA_ANTIGA_POR_PEDIDO:
        print("\n[PASSO 2.1] Filtro de consistência: coleta mais recente por codigo_pedido...")
        antes = len(df)
        df, _n = _filtrar_itens_coleta_antiga_por_pedido(df)
        print(f"[INFO] Registros após filtro: {len(df)} (antes: {antes})")
    else:
        print(
            "\n[PASSO 2.1] Filtro coleta por pedido DESATIVADO (GOLD_PEDIDO_COMPRA_DESATIVAR_FILTRO_COLETA)."
        )

    # 3. Processar dados para camada Gold
    print(f"\n[PASSO 3] Processando dados para camada Gold...")

    # Seleciona apenas as colunas desejadas, preenchendo com pd.NA se não existirem
    colunas_existentes = [col for col in COLUNAS_GOLD if col in df.columns]
    colunas_faltantes = [col for col in COLUNAS_GOLD if col not in df.columns]

    print(f"[INFO] Colunas encontradas: {len(colunas_existentes)}")
    if colunas_faltantes:
        print(f"[AVISO] Colunas não encontradas: {colunas_faltantes}")

    # Cria DataFrame Gold com colunas existentes
    gold = df[colunas_existentes].copy()

    # Adiciona colunas faltantes com valores nulos
    for col in colunas_faltantes:
        gold[col] = pd.NA

    # Reordena colunas conforme COLUNAS_GOLD
    gold = gold[COLUNAS_GOLD]

    # Remove linhas duplicadas usando chaves de negócio (pedido + produto)
    tamanho_antes = len(gold)

    # Identificar chaves para remoção de duplicatas
    colunas_chave = []
    if 'codigo_pedido' in gold.columns and 'produto_id' in gold.columns:
        colunas_chave = ['codigo_pedido', 'produto_id']
    elif 'codigo_pedido' in gold.columns and 'cod_item_pedido_compra' in gold.columns:
        colunas_chave = ['codigo_pedido', 'cod_item_pedido_compra']
    elif 'codigo_pedido' in gold.columns:
        colunas_chave = ['codigo_pedido']

    if colunas_chave:
        # Ordenar por data de processamento se existir (manter mais recente)
        colunas_ordenacao = []
        if 'datetime_coleta_dados' in gold.columns:
            colunas_ordenacao = ['datetime_coleta_dados']
        elif 'data_processamento_final' in gold.columns:
            colunas_ordenacao = ['data_processamento_final']
        elif 'data_consolidacao' in gold.columns:
            colunas_ordenacao = ['data_consolidacao']

        if colunas_ordenacao:
            # Converter para datetime se necessário
            for col in colunas_ordenacao:
                if gold[col].dtype == 'object':
                    try:
                        gold[col] = pd.to_datetime(
                            gold[col], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                    except:
                        pass
            if 'qtd_recebida' in gold.columns:
                gold['_qtd_rec_sort'] = pd.to_numeric(
                    gold['qtd_recebida'], errors='coerce')
            else:
                gold['_qtd_rec_sort'] = 0.0
            sort_keys = colunas_ordenacao + ['_qtd_rec_sort']
            asc = [False] * len(sort_keys)
            gold = gold.sort_values(
                sort_keys, ascending=asc, na_position='last')
            gold = gold.drop(columns=['_qtd_rec_sort'], errors='ignore')

        # Remover duplicatas mantendo a primeira (mais recente após ordenação)
        gold = gold.drop_duplicates(subset=colunas_chave, keep='first')
        duplicatas_removidas = tamanho_antes - len(gold)
        if duplicatas_removidas > 0:
            print(
                f"[INFO] Duplicatas removidas usando chaves {colunas_chave}: {duplicatas_removidas}")
    else:
        # Fallback: remover duplicatas completas
        gold = gold.drop_duplicates()
        duplicatas_removidas = tamanho_antes - len(gold)
        if duplicatas_removidas > 0:
            print(
                f"[AVISO] Duplicatas removidas (sem chaves específicas): {duplicatas_removidas}")

    # VALIDAÇÃO DE QUANTIDADES - Verificar pedidos específicos e possíveis problemas
    print(f"\n[VALIDAÇÃO] Validando quantidades de produtos...")

    if 'codigo_pedido' in gold.columns and 'qtd' in gold.columns:
        # Verificar pedidos específicos mencionados pelo usuário
        pedidos_validar = ['23479', '23299']
        pedidos_encontrados = []

        for pedido_id in pedidos_validar:
            pedido_str = str(pedido_id)
            pedido_filtrado = pd.DataFrame()

            # PRIORIDADE: Buscar por nr_pedido (número do pedido usado no BigQuery/view)
            if 'nr_pedido' in gold.columns:
                try:
                    # Tentar como numérico primeiro
                    gold_temp = gold.copy()
                    gold_temp['nr_pedido_num'] = pd.to_numeric(
                        gold_temp['nr_pedido'], errors='coerce')
                    pedido_filtrado = gold_temp[gold_temp['nr_pedido_num'] == int(
                        pedido_id)]
                except:
                    # Tentar como string
                    pedido_filtrado = gold[gold['nr_pedido'].astype(
                        str) == pedido_str]

            # Fallback: buscar por codigo_pedido se nr_pedido não encontrou
            if len(pedido_filtrado) == 0 and 'codigo_pedido' in gold.columns:
                if gold['codigo_pedido'].dtype == 'object' or gold['codigo_pedido'].dtype == 'string':
                    pedido_filtrado = gold[gold['codigo_pedido'].astype(
                        str) == pedido_str]
                else:
                    try:
                        pedido_filtrado = gold[gold['codigo_pedido'].astype(
                            int) == int(pedido_id)]
                    except:
                        pedido_filtrado = gold[gold['codigo_pedido'].astype(
                            str) == pedido_str]

            if len(pedido_filtrado) > 0:
                pedidos_encontrados.append(pedido_id)
                campo_usado = 'nr_pedido' if 'nr_pedido' in pedido_filtrado.columns else 'codigo_pedido'
                print(
                    f"\n[VALIDAÇÃO] Pedido {pedido_id} (campo: {campo_usado}):")
                print(f"  - Total de itens/produtos: {len(pedido_filtrado)}")
                if 'produto_id' in pedido_filtrado.columns:
                    print(
                        f"  - Produtos únicos: {pedido_filtrado['produto_id'].nunique()}")
                if 'qtd' in pedido_filtrado.columns:
                    qtd_soma = pedido_filtrado['qtd'].sum()
                    qtd_media = pedido_filtrado['qtd'].mean()
                    print(f"  - Soma de quantidades: {qtd_soma}")
                    print(f"  - Média de quantidades: {qtd_media:.2f}")
                    print(f"  - Quantidades individuais:")
                    for idx, row in pedido_filtrado.iterrows():
                        produto_info = f"Produto {row.get('produto_id', 'N/A')}" if 'produto_id' in row else "Item"
                        qtd_val = row.get('qtd', 'N/A')
                        print(f"    * {produto_info}: {qtd_val}")

                # Verificar duplicatas por produto_id
                if 'produto_id' in pedido_filtrado.columns:
                    produtos_duplicados = pedido_filtrado[pedido_filtrado.duplicated(
                        subset=['produto_id'], keep=False)]
                    if len(produtos_duplicados) > 0:
                        print(
                            f"  - ⚠️ ATENÇÃO: Produtos duplicados encontrados no pedido!")
                        produtos_dup = produtos_duplicados['produto_id'].unique(
                        )
                        for prod_id in produtos_dup:
                            prod_rows = pedido_filtrado[pedido_filtrado['produto_id'] == prod_id]
                            print(
                                f"    * Produto {prod_id} aparece {len(prod_rows)} vez(es)")
                            for idx, row in prod_rows.iterrows():
                                print(
                                    f"      - Qtd: {row.get('qtd', 'N/A')}, Item: {row.get('cod_item_pedido_compra', 'N/A')}")

        if len(pedidos_encontrados) < len(pedidos_validar):
            nao_encontrados = [
                p for p in pedidos_validar if p not in pedidos_encontrados]
            print(
                f"\n[AVISO] Pedidos não encontrados para validação: {nao_encontrados}")

        # Validação geral: verificar se há duplicatas de produto por pedido
        if 'produto_id' in gold.columns:
            duplicatas_produto_pedido = gold.groupby(
                ['codigo_pedido', 'produto_id']).size()
            duplicatas_produto_pedido = duplicatas_produto_pedido[duplicatas_produto_pedido > 1]
            if len(duplicatas_produto_pedido) > 0:
                print(
                    f"\n[AVISO] Encontrados {len(duplicatas_produto_pedido)} casos de produto duplicado no mesmo pedido!")
                print(f"  - Isso pode causar soma incorreta de quantidades")
                print(f"  - Primeiros 5 casos:")
                for (pedido, produto), count in duplicatas_produto_pedido.head().items():
                    pedido_data = gold[(gold['codigo_pedido'].astype(str) == str(pedido)) &
                                       (gold['produto_id'] == produto)]
                    print(
                        f"    * Pedido {pedido}, Produto {produto}: {count} ocorrências")
                    if 'qtd' in pedido_data.columns:
                        print(
                            f"      Quantidades: {pedido_data['qtd'].tolist()}")
            else:
                print(f"[OK] Nenhum produto duplicado encontrado por pedido")

    # Adiciona metadados Gold
    gold['data_processamento_gold'] = pd.Timestamp.now()
    gold['versao_gold'] = 'v1.0_fato'
    gold['camada'] = 'gold'

    # 4. Salvar arquivo Gold
    print(f"\n[PASSO 4] Salvando arquivo Gold...")
    gold.to_parquet(ARQUIVO_GOLD, index=False)
    print(f"[OK] Gold gerado com sucesso! Arquivo: {ARQUIVO_GOLD}")

    # 5. Upload para o GCS
    if SKIP_UPLOAD_GCS:
        print(
            "\n[PASSO 5] Upload GCS ignorado (GOLD_PEDIDO_COMPRA_SKIP_UPLOAD)."
        )
        upload_success = False
    else:
        from gcs_utils import upload_to_gcs
        from novo_projeto.config import GCS_BUCKET_NAME

        print(f"\n[PASSO 5] Fazendo upload para o GCS...")
        upload_success = upload_to_gcs(ARQUIVO_GOLD, GCS_BUCKET_NAME, GCS_PATH)

        if upload_success:
            print(
                f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}"
            )
        else:
            print(
                f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQUIVO_GOLD}"
            )

    # 6. Limpeza e resumo
    if (not SILVER_PARQUET_OVERRIDE) and os.path.exists(ARQUIVO_SILVER_LOCAL):
        os.unlink(ARQUIVO_SILVER_LOCAL)

    print(f"\n============================================================")
    print(f"[RESUMO GOLD FATO] Processamento concluído")
    print(f"============================================================")
    print(f"- Fonte: Tabela fato ({df.shape[0]} registros)")
    print(f"- Gold final: {gold.shape[0]} registros, {gold.shape[1]} colunas")
    print(f"- Colunas Gold: {len(COLUNAS_GOLD)} + 3 metadados")
    print(f"- Arquivo: {ARQUIVO_GOLD}")
    try:
        from novo_projeto.config import GCS_BUCKET_NAME as _gcs_bucket
    except Exception:
        _gcs_bucket = os.environ.get("GCS_BUCKET", "?")
    print(f"- GCS (bucket configurado): gs://{_gcs_bucket}/{GCS_PATH}")

    print(f"\n[INFO] Primeiras linhas do Gold:")
    print(gold.head())


if __name__ == '__main__':
    main()
