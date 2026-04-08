import pandas as pd
import os
import re
import json
from datetime import datetime
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs


def processar_arquivos_bronze():
    """Processa todos os arquivos Bronze de pedidos de compra do GCS"""
    print("[BRONZE] Processando arquivos de pedidos de compra do Bronze...")

    # Tenta localizar arquivos no caminho preferencial e no legado
    possiveis_prefixos = [
        "maq_prod_estoque/bronze/pedido_compra_encerrado/",
        "bronze/pedido_compra_encerrado/",
    ]

    bronze_root_prefix = None
    arquivos_bronze = []
    for prefix in possiveis_prefixos:
        arquivos_bronze = listar_arquivos_gcs(GCS_BUCKET_NAME, prefix)
        if arquivos_bronze:
            bronze_root_prefix = prefix if prefix.endswith(
                '/') else f"{prefix}/"
            break

    if not arquivos_bronze:
        print("[ERRO] Nenhum arquivo Bronze encontrado!")
        return None

    print(
        f"[INFO] Encontrados {len(arquivos_bronze)} arquivos Bronze em prefixo: {bronze_root_prefix if bronze_root_prefix else 'N/A'}")

    # Lista para armazenar todos os DataFrames
    dataframes_bronze = []

    # Filtrar apenas arquivos .parquet (raiz E subpastas processados/)
    arquivos_parquet = []
    for arquivo_gcs in arquivos_bronze:
        if arquivo_gcs.endswith('.parquet'):
            arquivos_parquet.append(arquivo_gcs)

    print(
        f"[INFO] Filtrados {len(arquivos_parquet)} arquivos .parquet (raiz + subpastas processados/)")

    # Processar cada arquivo Bronze (raiz + processados/)
    for i, arquivo_gcs in enumerate(arquivos_parquet, 1):
        nome_arquivo = arquivo_gcs.split('/')[-1]
        caminho_relativo = arquivo_gcs.replace(
            bronze_root_prefix, '') if bronze_root_prefix else arquivo_gcs
        print(
            f"[INFO] Processando arquivo {i}/{len(arquivos_parquet)}: {caminho_relativo}")

        # Baixar arquivo temporário
        arquivo_local = f"temp_{nome_arquivo}"

        if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_gcs, arquivo_local):
            try:
                # Carregar DataFrame
                df_arquivo = pd.read_parquet(arquivo_local)

                # Adicionar identificação do arquivo origem
                df_arquivo['arquivo_origem'] = nome_arquivo
                data_arquivo = extrair_data_do_nome_arquivo(nome_arquivo)
                df_arquivo['data_arquivo'] = data_arquivo

                dataframes_bronze.append(df_arquivo)
                print(f"  [OK] {len(df_arquivo)} registros carregados")

                # MOVIMENTAÇÃO DE ARQUIVOS DESABILITADA
                # Mover arquivo para pasta organizada por ANO/MÊS (apenas se estiver na raiz)
                # eh_arquivo_raiz = '/processados/' not in arquivo_gcs
                #
                # if eh_arquivo_raiz and data_arquivo:
                #     ano = data_arquivo.strftime('%Y')
                #     mes = data_arquivo.strftime('%m')
                #     destino_base = bronze_root_prefix
                #     pasta_destino = f"{destino_base}processados/{ano}/{mes}/"
                #     arquivo_destino = f"{pasta_destino}{nome_arquivo}"
                #
                #     print(f"  → Movendo para: {arquivo_destino}")
                #     if mover_arquivo_gcs(GCS_BUCKET_NAME, arquivo_gcs, arquivo_destino):
                #         print(f"  ✓ Arquivo movido com sucesso")
                #     else:
                #         print(f"  ✗ Erro ao mover arquivo")
                # elif not eh_arquivo_raiz:
                #     print(f"  ⊙ Arquivo já em pasta processados/ (não será movido)")

                # Remover arquivo temporário
                os.unlink(arquivo_local)

            except Exception as e:
                print(f"  [ERRO] ao processar {nome_arquivo}: {e}")
                if os.path.exists(arquivo_local):
                    os.unlink(arquivo_local)

    # Consolidar todos os DataFrames
    if dataframes_bronze:
        print(
            f"\n[CONSOLIDAÇÃO] Unindo {len(dataframes_bronze)} arquivos Bronze...")
        df_consolidado = pd.concat(dataframes_bronze, ignore_index=True)

        # Adicionar metadados de consolidação
        df_consolidado['data_consolidacao'] = datetime.now()
        df_consolidado['total_arquivos_origem'] = len(dataframes_bronze)

        print(f"[INFO] Consolidação concluída:")
        print(f"  - Total de registros: {len(df_consolidado)}")
        print(f"  - Total de colunas: {len(df_consolidado.columns)}")
        print(f"  - Arquivos processados: {len(dataframes_bronze)}")
        print(f"  - Total de arquivos .parquet: {len(arquivos_parquet)}")

        return df_consolidado
    else:
        print("[ERRO] Nenhum arquivo Bronze foi processado com sucesso!")
        return None


def explodir_produtos(df):
    """Explode os produtos para ter 1 produto por linha"""
    print("[TRANSFORMAÇÃO] Explodindo produtos (1 produto por linha)...")

    if 'produtos_consulta' not in df.columns:
        print("[AVISO] Coluna 'produtos_consulta' não encontrada")
        return df

    try:
        # Converte strings JSON para listas de dicionários
        df_temp = df.copy()
        df_temp['produtos_consulta'] = df_temp['produtos_consulta'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.strip() else []
        )

        # Explode a coluna de produtos (1 produto por linha)
        df_exploded = df_temp.explode(
            'produtos_consulta').reset_index(drop=True)

        # Remove linhas onde produtos_consulta é vazio ou None
        df_exploded = df_exploded[df_exploded['produtos_consulta'].notna()]
        df_exploded = df_exploded[df_exploded['produtos_consulta'] != '']

        print(
            f"[INFO] Produtos explodidos: {len(df)} pedidos -> {len(df_exploded)} linhas de produtos")
        return df_exploded

    except Exception as e:
        print(f"[ERRO] Erro ao explodir produtos: {e}")
        return df


def normalizar_colunas_json(df):
    """Normaliza colunas JSON do DataFrame (após explosão dos produtos)"""
    print("[TRANSFORMAÇÃO] Normalizando colunas JSON...")

    df_normalizado = df.copy()

    # 1. Normalizar produtos_consulta (agora são objetos únicos, não arrays)
    if 'produtos_consulta' in df_normalizado.columns:
        print("  - Normalizando produtos_consulta...")
        try:
            # Normaliza cada produto individual
            produtos_df = pd.json_normalize(
                df_normalizado['produtos_consulta'])

            # Adiciona prefixo
            produtos_df.columns = [
                f"produto_{col}" for col in produtos_df.columns]

            # Concatena com o DataFrame principal
            df_normalizado = pd.concat([df_normalizado.reset_index(
                drop=True), produtos_df.reset_index(drop=True)], axis=1)

            # Remove coluna original
            df_normalizado = df_normalizado.drop(columns=['produtos_consulta'])

        except Exception as e:
            print(f"    [AVISO] Erro ao normalizar produtos_consulta: {e}")

    # 2. Normalizar outras colunas JSON (parcelas, frete, etc.)
    outras_colunas_json = ['parcelas_consulta', 'frete_consulta', 'det', 'lista_parcelas',
                           'infoCadastro', 'informacoes_adicionais', 'total_pedido', 'frete', 'exportacao']

    for coluna in outras_colunas_json:
        if coluna in df_normalizado.columns:
            print(f"  - Normalizando coluna: {coluna}")
            try:
                # Converte strings JSON para dicionários
                df_temp = df_normalizado[coluna].apply(
                    lambda x: json.loads(x) if isinstance(
                        x, str) and x.strip() else {}
                )

                # Normaliza JSON para colunas planas
                df_json = pd.json_normalize(df_temp)

                # Adiciona prefixo da coluna original
                df_json.columns = [
                    f"{coluna}_{col}" for col in df_json.columns]

                # Concatena com o DataFrame principal
                df_normalizado = pd.concat([df_normalizado.reset_index(
                    drop=True), df_json.reset_index(drop=True)], axis=1)

                # Remove coluna original
                df_normalizado = df_normalizado.drop(columns=[coluna])

            except Exception as e:
                print(f"    [AVISO] Erro ao normalizar {coluna}: {e}")
                continue

    print(
        f"[INFO] Normalização concluída: {len(df_normalizado)} linhas, {len(df_normalizado.columns)} colunas")
    return df_normalizado


def renomear_colunas(df):
    """Aplica o mapeamento de renomeação das colunas"""
    print("[TRANSFORMAÇÃO] Renomeando colunas...")

    # Dicionario de mapeamento (original -> novo)
    mapeamento = {
        # Campos do cabeçalho do pedido
        'cCodCateg': 'cod_categoria',
        'cCodParc': 'cod_parcela',
        'cContato': 'nm_contato',
        'cEtapa': 'cod_etapa',
        'cNumero': 'nr_pedido',
        'dDtPrevisao': 'dt_previsao_entrega',
        'dIncData': 'dt_pedido',
        'nCodFor': 'cod_fornecedor',
        'nCodPed': 'codigo_pedido',

        # Campos dos produtos (após normalização JSON)
        'produto_cDescricao': 'de_produto',
        'produto_cNCM': 'cod_ncm',
        'produto_cProduto': 'cod_produto',
        'produto_cUnidade': 'un_produto',
        'produto_codigo_local_estoque': 'cod_local_estoque',
        'produto_nCodItem': 'cod_item_pedido_compra',
        'produto_nCodProd': 'produto_id',
        'produto_nDesconto': 'vlr_desconto',
        'produto_nDespesas': 'vlr_despesa',
        'produto_nFrete': 'vlr_frete',
        'produto_nQtde': 'qtd',
        'produto_nQtdeRec': 'qtd_recebida',
        'produto_nSeguro': 'vlr_seguro',
        'produto_nValMerc': 'vlr_total_produto_pedido',
        'produto_nValTot': 'vlr_total_item',
        'produto_nValUnit': 'vlr_unitario_pedido',

        # Campos de impostos
        'produto_nValorCofins': 'vlr_cofins',
        'produto_nValorIcms': 'vlr_icms',
        'produto_nValorIpi': 'vlr_ipi',
        'produto_nValorPis': 'vlr_pis',

        # Campos de parcelas
        'parcelas_consulta_dDtVenc': 'dt_vencimento_parcela',
        'parcelas_consulta_nValor': 'vlr_parcela',
        'parcelas_consulta_nParcela': 'nr_parcela',

        # Campos de frete
        'frete_consulta_nValor': 'vlr_frete_total',
        'frete_consulta_cTipo': 'tipo_frete',

        # Outros campos importantes
        'cCodIntFor': 'cod_integracacao_fornecedor',
        'cCodIntPed': 'cod_integracao_pedido',
        'cContrato': 'nr_contrato',
        'cIncHora': 'hr_inclusao',
        'cNumPedido': 'nr_pedido_interno',
        'dDtAlt': 'dt_alteracao',
        'dDtEntrega': 'dt_entrega',
        'nCodCC': 'cod_centro_custo',
        'nCodProj': 'cod_projeto',
        'nQtdeParcelas': 'qtd_parcelas',
        'nTotProd': 'vlr_total_produtos',
        'nTotPedido': 'vlr_total_pedido'
    }

    # Aplica renomeação apenas para colunas que existem
    colunas_existentes = {k: v for k,
                          v in mapeamento.items() if k in df.columns}

    print(f"[INFO] Renomeando {len(colunas_existentes)} colunas...")
    # Mostra apenas 5 exemplos
    for original, novo in list(colunas_existentes.items())[:5]:
        print(f"  - {original} -> {novo}")

    if len(colunas_existentes) > 5:
        print(f"  - ... e mais {len(colunas_existentes) - 5} colunas")

    df_renomeado = df.rename(columns=colunas_existentes)

    print(f"[INFO] Renomeação concluída: {len(df_renomeado.columns)} colunas")
    return df_renomeado


def listar_arquivos_gcs(bucket_name, prefix):
    """Lista todos os arquivos no GCS com o prefixo especificado"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        arquivos = []
        for blob in blobs:
            # Ignora "pastas" (blobs que terminam com /)
            if not blob.name.endswith('/'):
                arquivos.append(blob.name)

        return arquivos
    except Exception as e:
        print(f"[ERRO] Erro ao listar arquivos GCS: {e}")
        return []


def processar_arquivo_antigo(df_antigo):
    """Processa o arquivo antigo para ter estrutura compatível com o novo"""
    print("[TRANSFORMAÇÃO] Processando arquivo antigo para compatibilidade...")

    # Se o arquivo antigo não tem produtos explodidos, precisamos explodir
    if 'produtos_consulta' in df_antigo.columns:
        print("  - Explodindo produtos do arquivo antigo...")
        df_antigo = explodir_produtos(df_antigo)
        df_antigo = normalizar_colunas_json(df_antigo)
        df_antigo = renomear_colunas(df_antigo)

    # Adiciona identificador de origem
    df_antigo['origem_arquivo'] = 'silver_pedidos_compras_antigo'

    print(f"[INFO] Arquivo antigo processado: {len(df_antigo)} registros")
    return df_antigo


def concatenar_arquivos_silver():
    """Concatena os dois arquivos Silver em uma tabela fato unificada"""
    print("[CONCATENAÇÃO] Unificando arquivos Silver para tabela fato...")

    # 1. Baixar arquivo antigo do GCS
    print("\n[PASSO 1] Baixando arquivo antigo do GCS...")
    arquivo_antigo_gcs = "silver/pedidos/silver_pedidos_compras.parquet"
    arquivo_antigo_local = "temp_silver_pedidos_compras_antigo.parquet"

    df_antigo = None
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_antigo_gcs, arquivo_antigo_local):
        try:
            df_antigo = pd.read_parquet(arquivo_antigo_local)
            print(
                f"[INFO] Arquivo antigo carregado: {len(df_antigo)} registros, {len(df_antigo.columns)} colunas")

            # Processar arquivo antigo para compatibilidade
            df_antigo = processar_arquivo_antigo(df_antigo)

        except Exception as e:
            print(f"[AVISO] Erro ao carregar arquivo antigo: {e}")
            df_antigo = None
    else:
        print(
            "[AVISO] Arquivo antigo não encontrado, continuando apenas com arquivo novo")

    # 2. Baixar arquivo novo do GCS
    print("\n[PASSO 2] Baixando arquivo novo do GCS...")
    arquivo_novo_gcs = "silver/pedidos_compras/pedidos_compras.parquet"
    arquivo_novo_local = "temp_pedidos_compras_novo.parquet"

    df_novo = None
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_novo_gcs, arquivo_novo_local):
        try:
            df_novo = pd.read_parquet(arquivo_novo_local)
            print(
                f"[INFO] Arquivo novo carregado: {len(df_novo)} registros, {len(df_novo.columns)} colunas")

            # Adiciona identificador de origem
            df_novo['origem_arquivo'] = 'pedidos_compras_novo'

        except Exception as e:
            print(f"[ERRO] Erro ao carregar arquivo novo: {e}")
            return None
    else:
        print("[ERRO] Arquivo novo não encontrado")
        return None

    # 3. Concatenar os DataFrames
    print("\n[PASSO 3] Concatenando arquivos...")

    if df_antigo is not None and df_novo is not None:
        # Alinhar colunas (usar apenas colunas comuns)
        colunas_comuns = list(set(df_antigo.columns) & set(df_novo.columns))
        print(f"[INFO] Colunas comuns encontradas: {len(colunas_comuns)}")

        df_antigo_alinhado = df_antigo[colunas_comuns]
        df_novo_alinhado = df_novo[colunas_comuns]

        # Concatenar
        df_fato = pd.concat(
            [df_antigo_alinhado, df_novo_alinhado], ignore_index=True)

        print(f"[INFO] Concatenação realizada:")
        print(f"  - Arquivo antigo: {len(df_antigo_alinhado)} registros")
        print(f"  - Arquivo novo: {len(df_novo_alinhado)} registros")
        print(f"  - Total fato: {len(df_fato)} registros")

    elif df_novo is not None:
        # Apenas arquivo novo disponível
        df_fato = df_novo.copy()
        print(f"[INFO] Usando apenas arquivo novo: {len(df_fato)} registros")
    else:
        print("[ERRO] Nenhum arquivo válido encontrado")
        return None

    # 4. Adicionar metadados da tabela fato
    df_fato['data_processamento_fato'] = datetime.now()
    df_fato['versao_fato'] = 'v1.0'
    df_fato['camada'] = 'silver_fato'

    # 5. Remover duplicatas se existirem
    colunas_chave = [
        'codigo_pedido', 'produto_id'] if 'codigo_pedido' in df_fato.columns and 'produto_id' in df_fato.columns else None
    if colunas_chave and all(col in df_fato.columns for col in colunas_chave):
        tamanho_antes = len(df_fato)
        df_fato = df_fato.drop_duplicates(subset=colunas_chave, keep='last')
        duplicatas_removidas = tamanho_antes - len(df_fato)
        if duplicatas_removidas > 0:
            print(f"[INFO] Duplicatas removidas: {duplicatas_removidas}")

    # 6. Salvar tabela fato unificada
    print(f"\n[PASSO 4] Salvando tabela fato unificada...")
    arquivo_fato_local = "pedidos_compras_fato.parquet"
    df_fato.to_parquet(arquivo_fato_local, index=False)

    gcs_path_fato = "silver/pedidos_compras/pedidos_compras_fato.parquet"
    upload_success = upload_to_gcs(
        arquivo_fato_local, GCS_BUCKET_NAME, gcs_path_fato)

    if upload_success:
        print(
            f"[GCS] [OK] Tabela fato salva: gs://{GCS_BUCKET_NAME}/{gcs_path_fato}")
        print(f"[INFO] Registros na tabela fato: {len(df_fato)}")
        print(f"[INFO] Colunas na tabela fato: {len(df_fato.columns)}")

    # 7. Limpeza
    arquivos_temp = [arquivo_antigo_local,
                     arquivo_novo_local, arquivo_fato_local]
    for arquivo in arquivos_temp:
        if os.path.exists(arquivo):
            os.unlink(arquivo)

    print(f"\n[RESUMO CONCATENAÇÃO]")
    print(
        f"- Arquivo antigo: {len(df_antigo) if df_antigo is not None else 0} registros")
    print(
        f"- Arquivo novo: {len(df_novo) if df_novo is not None else 0} registros")
    print(f"- Tabela fato final: {len(df_fato)} registros")
    print(f"- Colunas finais: {len(df_fato.columns)}")

    return df_fato


def baixar_arquivo_gcs(bucket_name, source_blob_name, destination_file_name):
    """Baixa um arquivo do GCS para local temporário"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)
        print(
            f"[GCS] Arquivo baixado: {source_blob_name} -> {destination_file_name}")
        return True
    except Exception as e:
        print(f"[ERRO] Erro ao baixar {source_blob_name}: {e}")
        return False


def mover_arquivo_gcs(bucket_name, source_blob_name, destination_blob_name):
    """Move um arquivo dentro do GCS (copia + deleta original)"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)

        # Copia arquivo para novo local
        source_blob = bucket.blob(source_blob_name)
        destination_blob = bucket.blob(destination_blob_name)

        # Copia o conteúdo
        destination_blob.upload_from_string(source_blob.download_as_bytes())

        # Deleta o original
        source_blob.delete()

        print(
            f"[GCS] Arquivo movido: {source_blob_name} -> {destination_blob_name}")
        return True
    except Exception as e:
        print(f"[ERRO] Erro ao mover {source_blob_name}: {e}")
        return False


def extrair_data_do_nome_arquivo(nome_arquivo):
    """Extrai a data do nome do arquivo - suporta formatos: YYYY, YYYYMM, YYYYMMDD"""
    try:
        # Tentar extrair parte numérica do nome
        match = re.search(
            r'pedido_compra_encerrado_(\d+)\.parquet', nome_arquivo)
        if match:
            data_str = match.group(1)

            # Formato anual: YYYY (4 dígitos) -> 01/01/YYYY
            if len(data_str) == 4:
                data = datetime.strptime(data_str, '%Y')
                return data.replace(month=1, day=1)

            # Formato mensal: YYYYMM (6 dígitos) -> 01/MM/YYYY
            elif len(data_str) == 6:
                data = datetime.strptime(data_str, '%Y%m')
                return data.replace(day=1)

            # Formato diário: YYYYMMDD (8 dígitos)
            elif len(data_str) == 8:
                data = datetime.strptime(data_str, '%Y%m%d')
                return data

            else:
                print(f"[AVISO] Formato desconhecido para: {nome_arquivo}")
                return None
        else:
            print(
                f"[AVISO] Não foi possível extrair data do arquivo: {nome_arquivo}")
            return None
    except Exception as e:
        print(f"[ERRO] Erro ao extrair data de {nome_arquivo}: {e}")
        return None


# Script para criar versão de validação temporária

# Salvar a função main original e criar uma nova
def main_bronze_para_silver():
    """Processa arquivos Bronze da raiz, move para pastas organizadas e gera staging"""
    print("[STAGING] Processando arquivos Bronze da raiz e organizando...")

    # 1. Processar arquivos Bronze
    print("\n[PASSO 1] Processando arquivos Bronze...")
    df_consolidado = processar_arquivos_bronze()

    if df_consolidado is None:
        print("[ERRO] Falha no processamento dos arquivos Bronze.")
        return

    # 2. Aplicar transformações
    print(f"\n[PASSO 2] Aplicando transformações...")
    print(
        f"[INFO] DataFrame inicial: {len(df_consolidado)} registros, {len(df_consolidado.columns)} colunas")

    # Explosão de produtos
    df_consolidado = explodir_produtos(df_consolidado)

    # Normalização JSON
    df_consolidado = normalizar_colunas_json(df_consolidado)

    # Renomeação de colunas
    df_consolidado = renomear_colunas(df_consolidado)

    print(
        f"[INFO] DataFrame final: {len(df_consolidado)} registros, {len(df_consolidado.columns)} colunas")

    # Adicionar coluna fl_encerrado = 'S'
    df_consolidado['fl_encerrado'] = 'S'
    print(f"[INFO] Coluna fl_encerrado adicionada com valor 'S'")

    # 3. Salvar arquivo staging
    print(f"\n[PASSO 3] Salvando arquivo staging...")
    arquivo_staging_local = "pedido_compra_encerrado.parquet"
    df_consolidado.to_parquet(arquivo_staging_local, index=False)

    gcs_path_staging = "silver/pedidos_compras/pedido_compra_encerrado.parquet"
    upload_success = upload_to_gcs(
        arquivo_staging_local, GCS_BUCKET_NAME, gcs_path_staging)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo staging salvo: gs://{GCS_BUCKET_NAME}/{gcs_path_staging}")
        print(f"[INFO] Registros salvos: {len(df_consolidado)}")

    # 4. Limpeza
    arquivos_temp = [arquivo_staging_local]
    for arquivo in arquivos_temp:
        if os.path.exists(arquivo):
            os.unlink(arquivo)

    # 5. Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Processamento Staging concluído")
    print(f"{'='*60}")
    print(
        f"- Origem: gs://{GCS_BUCKET_NAME}/bronze/pedido_compra_encerrado/ (raiz + processados/)")
    print(
        f"- Movimentação de arquivos: DESABILITADA (arquivos permanecem no local original)")
    print(f"- Staging gerado: gs://{GCS_BUCKET_NAME}/{gcs_path_staging}")
    print(f"- Registros processados: {len(df_consolidado)}")
    print(f"[INFO] [OK] Arquivo final sera gerado pelo nsilver_pedido_compra.py")
    print(f"- Colunas finais: {len(df_consolidado.columns)}")

    print(
        f"\n[OK] Staging concluído! Execute nsilver_pedido_compra.py para consolidar.")
    return df_consolidado


def main_validacao():
    print("[VALIDAÇÃO] Processando arquivo staging para arquivo final...")

    # 1. Baixar arquivo staging do GCS
    print("\n[PASSO 1] Baixando arquivo staging do GCS...")
    arquivo_staging_gcs = "silver/pedidos_compras/pedido_compra_encerrado.parquet"
    arquivo_staging_local = "temp_pedido_compra_encerrado.parquet"

    if not baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_staging_gcs, arquivo_staging_local):
        print("[ERRO] Não foi possível baixar o arquivo staging.")
        return

    # 2. Carregar DataFrame do arquivo staging
    print("\n[PASSO 2] Carregando dados do arquivo staging...")
    try:
        df_consolidado = pd.read_parquet(arquivo_staging_local)
        print(
            f"[INFO] DataFrame carregado: {len(df_consolidado)} registros, {len(df_consolidado.columns)} colunas")
        print(
            f"[INFO] Primeiras colunas: {list(df_consolidado.columns)[:10]}{'...' if len(df_consolidado.columns) > 10 else ''}")

        # Mostra algumas estatísticas
        print(f"[INFO] Amostra dos dados:")
        print(f"  - Colunas totais: {len(df_consolidado.columns)}")
        print(f"  - Registros totais: {len(df_consolidado)}")
        if 'arquivo_origem' in df_consolidado.columns:
            print(
                f"  - Arquivos origem únicos: {df_consolidado['arquivo_origem'].nunique()}")
        if 'cod_fornecedor' in df_consolidado.columns:
            print(
                f"  - Fornecedores únicos: {df_consolidado['cod_fornecedor'].nunique()}")

    except Exception as e:
        print(f"[ERRO] Erro ao carregar arquivo staging: {e}")
        return

    # 3. Aplicar transformações finais
    print(f"\n[PASSO 3] Aplicando transformações finais...")

    # 3.1. Explodir produtos (1 produto por linha)
    df_consolidado = explodir_produtos(df_consolidado)

    # 3.2. Normalizar colunas JSON
    df_consolidado = normalizar_colunas_json(df_consolidado)

    # 3.3. Renomear colunas
    df_consolidado = renomear_colunas(df_consolidado)

    # 3.4. Adiciona metadados finais
    df_consolidado['data_processamento_final'] = datetime.now()
    df_consolidado['versao'] = 'final'
    df_consolidado['status'] = 'processado'

    print(
        f"[INFO] DataFrame final: {len(df_consolidado)} registros, {len(df_consolidado.columns)} colunas")

    # 4. Limpeza de arquivos temporários
    print(f"\n[PASSO 4] Limpeza...")
    try:
        os.remove(arquivo_staging_local)
    except:
        pass

    # 5. Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Validação concluída")
    print(f"{'='*60}")
    print(f"- Arquivo origem: gs://{GCS_BUCKET_NAME}/{arquivo_staging_gcs}")
    print(f"- Registros processados: {len(df_consolidado)}")
    print(f"- Colunas finais: {len(df_consolidado.columns)}")

    print(f"\n[OK] Validação concluída com sucesso!")


def main():
    """Função principal com opções de processamento"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--concatenar":
        # Modo concatenacao: une os dois arquivos Silver
        print("[MODO] Concatenacao de arquivos Silver")
        concatenar_arquivos_silver()
    elif len(sys.argv) > 1 and sys.argv[1] == "--validacao":
        # Modo validacao: processa staging para final
        print("[MODO] Processamento de validacao (staging -> final)")
        main_validacao()
    else:
        # Modo padrao: processamento direto Bronze -> Silver
        print("[MODO] Processamento direto Bronze -> Silver")
        main_bronze_para_silver()


if __name__ == "__main__":
    main()
