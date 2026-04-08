import pandas as pd
import os
from pathlib import Path
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

ARQUIVO_SILVER = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais.parquet"
ARQUIVO_GOLD = "maq_prod_estoque/gold/nota_fiscal/gold_nfconsultar.parquet"

# Configuraes do GCS
BUCKET_NAME = GCS_BUCKET_NAME
GCS_PATH = "gold/nota_fiscal/gold_nfconsultar.parquet"

# Colunas a serem mantidas (apenas as especificadas no mapeamento)
COLUNAS_OK = [
    # Colunas conforme especificao do mapeamento De/Para
    'ide.nNF',                  # nr_pedido
    'ide.dEmi',                 # dt_pedido e dt_previsao_entrega
    'nfDestInt.nCodCli',        # cod_fornecedor
    'compl.nIdNF',              # codigo_pedido
    'det_prod.cProd',           # cod_produto
    'det_prod.uCom',            # un_produto
    'det_nfProdInt.nCodItem',   # cod_item_pedido_compra
    'det_nfProdInt.nCodProd',   # produto_id
    'det_prod.vDesc',           # vlr_desconto
    'total.ICMSTot.vOutro',     # vlr_despesa
    'det_prod.vFrete',          # vlr_frete
    'det_prod.qCom',            # qtd e qtd_recebida
    'det_prod.vSeg',            # vlr_seguro
    'total.ICMSTot.vNF',        # vlr_total_produto_pedido
    'det_prod.vTotItem',        # vlr_total_item
    'det_prod.vUnCom',          # vlr_unitario_pedido
    'det_prod.vCOFINS',         # vlr_cofins
    'det_prod.vICMS',           # vlr_icms
    'det_prod.vIPI',            # vlr_ipi
    'det_prod.vPIS',            # vlr_pis
    'det_prod.vICMSST',         # vlr_icms_st
    'titulo_dDtVenc',           # dt_vencimento
    'titulo_nParcela',          # nr_parcela
    'compl.cModFrete',          # cod_frete_modalidade
]

# Removidas configuraes de rateio e colunas extras - mantendo apenas as especificadas

# Dicionrio para renomear colunas conforme especificao (De/Para)
MAPEAMENTO_RENOMEAR = {
    # Mapeamento conforme especificao do item 1
    'ide.nNF': 'nr_pedido',
    'ide.dEmi': 'dt_pedido',  # Mesma coluna para dt_previsao_entrega e dt_pedido
    'nfDestInt.nCodCli': 'cod_fornecedor',
    'compl.nIdNF': 'codigo_pedido',
    'det_prod.cProd': 'cod_produto',
    'det_prod.uCom': 'un_produto',
    'det_nfProdInt.nCodItem': 'cod_item_pedido_compra',
    'det_nfProdInt.nCodProd': 'produto_id',
    'det_prod.vDesc': 'vlr_desconto',
    'total.ICMSTot.vOutro': 'vlr_despesa',
    'det_prod.vFrete': 'vlr_frete',
    'det_prod.qCom': 'qtd',  # Mesma coluna para qtd e qtd_recebida
    'det_prod.vSeg': 'vlr_seguro',
    'total.ICMSTot.vNF': 'vlr_total_produto_pedido',
    'det_prod.vTotItem': 'vlr_total_item',
    'det_prod.vUnCom': 'vlr_unitario_pedido',
    'det_prod.vCOFINS': 'vlr_cofins',
    'det_prod.vICMS': 'vlr_icms',
    'det_prod.vIPI': 'vlr_ipi',
    'det_prod.vPIS': 'vlr_pis',
    'det_prod.vICMSST': 'vlr_icms_st',
    'titulo_dDtVenc': 'dt_vencimento',
    'titulo_nParcela': 'nr_parcela',
    'compl.cModFrete': 'cod_frete_modalidade'
}


def main():
    print("[PROCESSANDO] Iniciando processamento GOLD - Notas Fiscais")

    # Verifica se o arquivo silver existe
    if not os.path.exists(ARQUIVO_SILVER):
        print(f"[ERRO] Arquivo silver no encontrado: {ARQUIVO_SILVER}")
        return

    # Carrega dados silver
    df = pd.read_parquet(ARQUIVO_SILVER)
    print(f"[INFO] Registros carregados: {len(df)}")

    # Diagnstico: Verifica cdigos negativos na camada Silver
    print("[DIAGNSTICO] Verificando cdigos negativos na camada Silver...")
    campos_originais = ['nfDestInt.nCodCli', 'compl.nIdNF',
                        'det_prod.cProd', 'det_nfProdInt.nCodItem', 'det_nfProdInt.nCodProd']
    for campo in campos_originais:
        if campo in df.columns:
            # Verifica valores negativos
            df_temp = pd.to_numeric(df[campo], errors='coerce')
            negativos = (df_temp < 0).sum()
            if negativos > 0:
                print(
                    f"  AVISO {campo}: {negativos} valores negativos encontrados na Silver")
                # Mostra alguns exemplos
                exemplos = df_temp[df_temp < 0].head(3).tolist()
                print(f"    Exemplos: {exemplos}")
            else:
                print(f"  OK {campo}: Nenhum valor negativo na Silver")

    # Seleciona apenas as colunas especificadas
    colunas_existentes = [col for col in COLUNAS_OK if col in df.columns]
    df_gold = df[colunas_existentes].copy()
    print(f'[INFO] Colunas selecionadas: {len(colunas_existentes)}')
    print(f'[INFO] Colunas em df_gold: {df_gold.columns.tolist()}')

    # Renomeia as colunas conforme o dicionrio
    df_gold = df_gold.rename(columns=MAPEAMENTO_RENOMEAR)

    # Diagnstico: Verifica cdigos negativos aps renomeamento
    print("[DIAGNSTICO] Verificando cdigos negativos aps renomeamento...")
    campos_renomeados = ['cod_fornecedor', 'codigo_pedido',
                         'cod_produto', 'cod_item_pedido_compra', 'produto_id']
    for campo in campos_renomeados:
        if campo in df_gold.columns:
            # Verifica valores negativos
            df_temp = pd.to_numeric(df_gold[campo], errors='coerce')
            negativos = (df_temp < 0).sum()
            if negativos > 0:
                print(
                    f"  AVISO {campo}: {negativos} valores negativos encontrados aps renomeamento")
                # Mostra alguns exemplos
                exemplos = df_temp[df_temp < 0].head(3).tolist()
                print(f"    Exemplos: {exemplos}")
            else:
                print(f"  OK {campo}: Nenhum valor negativo aps renomeamento")

    # Filtro CFOP 3.102 removido - processando todos os registros

    # Adiciona colunas duplicadas conforme especificao
    print("[INFO] Adicionando colunas duplicadas conforme especificao...")

    # dt_previsao_entrega = ide.dEmi (mesmo valor que dt_pedido)
    if 'dt_pedido' in df_gold.columns:
        df_gold['dt_previsao_entrega'] = df_gold['dt_pedido']
        print("   Adicionada coluna: dt_previsao_entrega")

    # qtd_recebida = det_prod.qCom (mesmo valor que qtd)
    if 'qtd' in df_gold.columns:
        df_gold['qtd_recebida'] = df_gold['qtd']
        print("   Adicionada coluna: qtd_recebida")

    # Converte colunas de data para formato string (dd/mm/YYYY)
    print("[INFO] Convertendo colunas de data...")
    colunas_data_string = ['dt_pedido', 'dt_previsao_entrega']

    for col in colunas_data_string:
        if col in df_gold.columns:
            try:
                # Converte para datetime e depois para string no formato dd/mm/YYYY
                df_gold[col] = pd.to_datetime(
                    df_gold[col], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                print(f"   Convertida coluna: {col}  STRING (dd/mm/YYYY)")
            except Exception as e:
                print(f"   Erro ao converter {col}: {e}")
        else:
            print(f"   Coluna {col} no encontrada")

    # Converte dt_vencimento para integer
    if 'dt_vencimento' in df_gold.columns:
        try:
            # Converte para datetime, depois para string no formato YYYYMMDD, e finalmente para integer
            df_gold['dt_vencimento'] = pd.to_datetime(
                df_gold['dt_vencimento'], errors='coerce', dayfirst=True).dt.strftime('%Y%m%d').astype('Int64')
            print(f"   Convertida coluna: dt_vencimento  INTEGER (YYYYMMDD)")
        except Exception as e:
            print(f"   Erro ao converter dt_vencimento: {e}")
    else:
        print(f"   Coluna dt_vencimento no encontrada")

    # Adiciona colunas com valores padro
    print("[INFO] Adicionando colunas com valores padro...")
    df_gold['cod_etapa'] = '70'
    df_gold['nr_dias'] = 0

    # Adiciona colunas de metadados conforme especificao
    from datetime import datetime
    df_gold['data_processamento_gold'] = datetime.now()
    df_gold['versao_gold'] = '1.0'
    df_gold['camada'] = 'gold'

    print("   Adicionada coluna: cod_etapa = '70' (string)")
    print("   Adicionada coluna: nr_dias = 0")
    print("   Adicionada coluna: data_processamento_gold = timestamp atual")
    print("   Adicionada coluna: versao_gold = '1.0'")
    print("   Adicionada coluna: camada = 'gold'")

    # Converte tipos de dados conforme especificao da tabela
    print("[INFO] Convertendo tipos de dados conforme especificao...")

    # STRING fields
    string_fields = ['cod_etapa', 'nr_pedido', 'dt_previsao_entrega',
                     'dt_pedido', 'cod_produto', 'un_produto', 'versao_gold', 'camada']
    for field in string_fields:
        if field in df_gold.columns:
            df_gold[field] = df_gold[field].astype(str)
            print(f"   {field}: STRING")

    # INTEGER fields (campos que devem ser INTEGER conforme especificao)
    integer_fields = [
        'cod_fornecedor', 'codigo_pedido', 'cod_item_pedido_compra', 'produto_id',
        'vlr_despesa', 'vlr_frete', 'qtd', 'qtd_recebida', 'vlr_seguro',
        'vlr_cofins', 'vlr_pis', 'vlr_icms_st', 'dt_vencimento', 'nr_dias',
        'nr_parcela', 'cod_frete_modalidade'
    ]
    for field in integer_fields:
        if field in df_gold.columns:
            try:
                # Converso para INTEGER compatvel com BigQuery
                df_gold[field] = pd.to_numeric(df_gold[field], errors='coerce')
                # Usa int64 nativo que no causa problemas de overflow
                df_gold[field] = df_gold[field].astype('int64')
                print(f"   {field}: INTEGER")
            except Exception as e:
                print(f"   Erro ao converter {field}: {e}")

    # Corrige cdigos negativos para valores absolutos
    print("[INFO] Corrigindo cdigos negativos...")
    campos_codigo = ['cod_fornecedor', 'codigo_pedido',
                     'cod_item_pedido_compra', 'produto_id']
    for campo in campos_codigo:
        if campo in df_gold.columns:
            # Conta valores negativos antes da correo
            negativos = (df_gold[campo] < 0).sum()
            if negativos > 0:
                print(f"   {campo}: {negativos} valores negativos encontrados")
                # Converte valores negativos para absolutos
                df_gold[campo] = df_gold[campo].abs()
                print(
                    f"   {campo}: Valores negativos convertidos para absolutos")
            else:
                print(f"   {campo}: Nenhum valor negativo encontrado")

    # FLOAT fields
    float_fields = ['vlr_desconto', 'vlr_total_produto_pedido',
                    'vlr_total_item', 'vlr_unitario_pedido', 'vlr_icms', 'vlr_ipi']
    for field in float_fields:
        if field in df_gold.columns:
            try:
                # Primeiro converte para numeric, depois para float64
                df_gold[field] = pd.to_numeric(df_gold[field], errors='coerce')
                df_gold[field] = df_gold[field].astype('float64')
                print(f"   {field}: FLOAT")
            except Exception as e:
                print(f"   Erro ao converter {field}: {e}")

    # TIMESTAMP fields
    timestamp_fields = ['data_processamento_gold']
    for field in timestamp_fields:
        if field in df_gold.columns:
            try:
                # Converte para datetime64[ns] que  compatvel com TIMESTAMP no BigQuery
                df_gold[field] = pd.to_datetime(df_gold[field])
                print(f"   {field}: TIMESTAMP")
            except Exception as e:
                print(f"   Erro ao converter {field}: {e}")

    # Coluna CFOP removida da lista - no  mais necessria

    # Mostra tipos de dados finais conforme especificao
    print(f"\n[INFO] Tipos de dados finais conforme especificao:")
    todas_colunas = ['cod_etapa', 'nr_pedido', 'dt_previsao_entrega', 'dt_pedido', 'cod_fornecedor',
                     'codigo_pedido', 'cod_produto', 'un_produto', 'cod_item_pedido_compra', 'produto_id',
                     'vlr_desconto', 'vlr_despesa', 'vlr_frete', 'qtd', 'qtd_recebida', 'vlr_seguro',
                     'vlr_total_produto_pedido', 'vlr_total_item', 'vlr_unitario_pedido', 'vlr_cofins',
                     'vlr_icms', 'vlr_ipi', 'vlr_pis', 'vlr_icms_st', 'dt_vencimento', 'nr_dias',
                     'nr_parcela', 'cod_frete_modalidade', 'data_processamento_gold', 'versao_gold', 'camada']

    for col in todas_colunas:
        if col in df_gold.columns:
            tipo = df_gold[col].dtype
            print(f"  - {col}: {tipo}")

    # Mostra colunas finais
    print(f"\n[INFO] Colunas finais no DataFrame GOLD: {len(df_gold.columns)}")
    for col in sorted(df_gold.columns):
        print(f"  - {col}")
    print(f"[INFO] Total de registros: {len(df_gold)}")

    # # Cria a coluna fk_nf com a juno de cod_cliente e cfop
    # if 'cod_cliente' in df_gold.columns and 'cfop' in df_gold.columns:
    #     df_gold['fk_nf'] = df_gold['cod_cliente'].astype(str) + '_' + df_gold['cfop'].astype(str)
    # else:
    #     print(' No foi possvel criar fk_nf: cod_cliente ou cfop no existem no DataFrame.')

    # Salva o arquivo gold
    os.makedirs(os.path.dirname(ARQUIVO_GOLD), exist_ok=True)
    df_gold.to_parquet(ARQUIVO_GOLD, index=False)
    print(f"\n[OK] Arquivo GOLD gerado: {ARQUIVO_GOLD}")

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(ARQUIVO_GOLD, BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponivel no GCS: gs://{BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local esta disponivel: {ARQUIVO_GOLD}")

    print("\n[INFO] Amostra dos dados:")
    print(df_gold.head())


if __name__ == "__main__":
    main()
