import pandas as pd
import os
from datetime import datetime
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs


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
        print(f"[AVISO] Erro ao baixar {source_blob_name}: {e}")
        return False


def main():
    """
    Concatena dados existentes do Silver com os novos dados processados pelo staging
    """
    print("[CONCATENAÇÃO] Unificando dados Silver existentes com novos...")

    # Caminhos dos arquivos
    arquivo_existente_gcs = "silver/pedidos_compras/pedidos_compras.parquet"
    arquivo_staging_gcs = "silver/pedidos_compras/pedidos_compras_stg.parquet"
    arquivo_encerrado_gcs = "silver/pedidos_compras/pedido_compra_encerrado.parquet"
    arquivo_cancelado_gcs = "silver/pedidos_compras/pedidos_compras_cancelado.parquet"
    arquivo_final_gcs = "silver/pedidos_compras/pedidos_compras.parquet"

    arquivo_existente_local = "temp_existente.parquet"
    arquivo_staging_local = "temp_staging.parquet"
    arquivo_encerrado_local = "temp_encerrado.parquet"
    arquivo_cancelado_local = "temp_cancelado.parquet"
    arquivo_final_local = "pedidos_compras_consolidado.parquet"

    df_existente = None
    df_staging = None
    df_encerrado = None
    df_cancelado = None

    # 1. Tentar baixar arquivo existente
    print("\n[PASSO 1] Verificando dados existentes...")
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_existente_gcs, arquivo_existente_local):
        try:
            df_existente = pd.read_parquet(arquivo_existente_local)
            print(
                f"[INFO] Dados existentes carregados: {len(df_existente)} registros")
            # Adicionar identificador de origem
            df_existente['origem_processamento'] = 'existente'
        except Exception as e:
            print(f"[AVISO] Erro ao carregar dados existentes: {e}")
            df_existente = None
    else:
        print("[INFO] Nenhum arquivo existente encontrado - será criado novo")

    # 2. Baixar dados do staging
    print("\n[PASSO 2] Carregando novos dados do staging...")
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_staging_gcs, arquivo_staging_local):
        try:
            df_staging = pd.read_parquet(arquivo_staging_local)
            print(
                f"[INFO] Novos dados carregados: {len(df_staging)} registros")
            # Adicionar identificador de origem
            df_staging['origem_processamento'] = 'novo'
        except Exception as e:
            print(f"[ERRO] Erro ao carregar dados do staging: {e}")
            return False
    else:
        print("[ERRO] Não foi possível carregar dados do staging")
        return False

    # 2.1. Baixar dados de pedidos encerrados
    print("\n[PASSO 2.1] Carregando dados de pedidos encerrados...")
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_encerrado_gcs, arquivo_encerrado_local):
        try:
            df_encerrado = pd.read_parquet(arquivo_encerrado_local)
            print(
                f"[INFO] Dados encerrados carregados: {len(df_encerrado)} registros")
            print(f"[INFO] Colunas disponíveis: {list(df_encerrado.columns)}")

            # Debug: verificar se o pedido 23423 está presente
            if 'codigo_pedido' in df_encerrado.columns:
                pedido_debug = df_encerrado[df_encerrado['codigo_pedido']
                                            == '23423']
                print(
                    f"[DEBUG] Pedido 23423 encontrado em encerrados: {len(pedido_debug)} registros")
                if len(pedido_debug) > 0:
                    print(
                        f"[DEBUG] Colunas do pedido: {list(pedido_debug.columns)}")
                    print(
                        f"[DEBUG] Valores fl_encerrado: {pedido_debug['fl_encerrado'].unique() if 'fl_encerrado' in pedido_debug.columns else 'N/A'}")
            elif 'nr_pedido' in df_encerrado.columns:
                pedido_debug = df_encerrado[df_encerrado['nr_pedido']
                                            == '23423']
                print(
                    f"[DEBUG] Pedido 23423 encontrado em encerrados (nr_pedido): {len(pedido_debug)} registros")
        except Exception as e:
            print(f"[AVISO] Erro ao carregar dados encerrados: {e}")
            df_encerrado = None
    else:
        print("[AVISO] Arquivo de pedidos encerrados não encontrado")

    # 2.2. Baixar dados de pedidos cancelados
    print("\n[PASSO 2.2] Carregando dados de pedidos cancelados...")
    if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_cancelado_gcs, arquivo_cancelado_local):
        try:
            df_cancelado = pd.read_parquet(arquivo_cancelado_local)
            print(
                f"[INFO] Dados cancelados carregados: {len(df_cancelado)} registros")
        except Exception as e:
            print(f"[AVISO] Erro ao carregar dados cancelados: {e}")
            df_cancelado = None
    else:
        print("[AVISO] Arquivo de pedidos cancelados não encontrado")

    # 3. Concatenar os dados
    print("\n[PASSO 3] Concatenando dados...")

    if df_existente is not None:
        # Snapshot por pedido: todo codigo_pedido que aparece no staging substitui
        # as linhas daquele pedido no silver existente (remove itens obsoletos na OMIE).
        if "codigo_pedido" in df_staging.columns and "codigo_pedido" in df_existente.columns:
            pedidos_substituir = set(
                df_staging.loc[df_staging["codigo_pedido"].notna(), "codigo_pedido"]
                .astype(str)
                .str.strip()
            )
            pedidos_substituir.discard("")
            pedidos_substituir.discard("nan")
            if pedidos_substituir:
                n_antes = len(df_existente)
                existente_cod = df_existente["codigo_pedido"].astype(str).str.strip()
                manter = ~existente_cod.isin(pedidos_substituir)
                df_existente = df_existente.loc[manter].copy()
                rem = n_antes - len(df_existente)
                if rem > 0:
                    print(
                        f"[INFO] Substituicao por pedido (staging): removidos {rem} registros do "
                        f"silver existente; {len(pedidos_substituir)} codigo_pedido atualizados pelo snapshot do staging."
                    )
                else:
                    print(
                        "[INFO] Substituicao por pedido: nenhuma linha removida do existente "
                        "(pedidos do staging ainda nao constavam no arquivo anterior)."
                    )

        # Verificar colunas comuns
        colunas_comuns = list(set(df_existente.columns)
                              & set(df_staging.columns))
        print(f"[INFO] Colunas comuns encontradas: {len(colunas_comuns)}")

        # Debug: verificar se pedido 23396 está em ambos
        pedido_teste = '23396'
        if 'nr_pedido' in df_existente.columns:
            pedido_existente_count = len(
                df_existente[df_existente['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Pedido {pedido_teste} no arquivo existente: {pedido_existente_count} registros")

        if 'nr_pedido' in df_staging.columns:
            pedido_staging_count = len(
                df_staging[df_staging['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Pedido {pedido_teste} no arquivo staging: {pedido_staging_count} registros")

        # Alinhar DataFrames usando apenas colunas comuns
        df_existente_alinhado = df_existente[colunas_comuns]
        df_staging_alinhado = df_staging[colunas_comuns]

        # Debug: verificar após alinhamento
        if 'nr_pedido' in colunas_comuns:
            pedido_exist_align = len(
                df_existente_alinhado[df_existente_alinhado['nr_pedido'] == pedido_teste])
            pedido_stag_align = len(
                df_staging_alinhado[df_staging_alinhado['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Após alinhamento - Existente: {pedido_exist_align}, Staging: {pedido_stag_align}")

        # Concatenar
        df_consolidado = pd.concat(
            [df_existente_alinhado, df_staging_alinhado], ignore_index=True)

        # Debug: verificar após concatenação
        if 'nr_pedido' in df_consolidado.columns:
            pedido_consolidado_count = len(
                df_consolidado[df_consolidado['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Pedido {pedido_teste} após concatenação: {pedido_consolidado_count} registros")

        print(f"[INFO] Concatenação realizada:")
        print(f"  - Dados existentes: {len(df_existente_alinhado)} registros")
        print(f"  - Dados novos: {len(df_staging_alinhado)} registros")
        print(f"  - Total consolidado: {len(df_consolidado)} registros")

    else:
        # Apenas dados novos
        df_consolidado = df_staging.copy()
        print(
            f"[INFO] Usando apenas dados novos: {len(df_consolidado)} registros")

    # 4. Remover duplicatas se existirem (manter o mais recente)
    print("\n[PASSO 4] Validando e removendo duplicatas...")

    # Identificar colunas-chave para remoção de duplicatas (pedido + item)
    colunas_chave = []
    coluna_data = None

    # Prioridade: codigo_pedido + produto_id (chave mais específica)
    if 'codigo_pedido' in df_consolidado.columns and 'produto_id' in df_consolidado.columns:
        colunas_chave = ['codigo_pedido', 'produto_id']
    elif 'codigo_pedido' in df_consolidado.columns and 'cod_item_pedido_compra' in df_consolidado.columns:
        colunas_chave = ['codigo_pedido', 'cod_item_pedido_compra']
    elif 'codigo_pedido' in df_consolidado.columns:
        colunas_chave = ['codigo_pedido']

    # Identificar coluna de data para ordenação (manter o mais recente)
    # PRIORIDADE: datetime_coleta_dados > datetime_processamento > data_coleta > data_arquivo
    # IMPORTANTE: datetime_coleta_dados captura atualizações manuais e coletas recentes
    colunas_data_possiveis = ['datetime_coleta_dados', 'datetime_processamento', 'data_coleta',
                              'data_processamento_final', 'data_arquivo', 'dt_pedido', 'dt_alteracao']
    for col in colunas_data_possiveis:
        if col in df_consolidado.columns:
            coluna_data = col
            break

    if colunas_chave:
        tamanho_antes = len(df_consolidado)

        print(f"[INFO] Chaves de duplicata: {colunas_chave}")
        print(f"[INFO] Coluna de ordenação: {coluna_data}")

        # Verificar duplicatas antes da remoção
        duplicatas_antes = df_consolidado.duplicated(
            subset=colunas_chave).sum()
        print(f"[INFO] Duplicatas encontradas: {duplicatas_antes}")

        # Debug: verificar pedido teste antes da remoção de duplicatas
        pedido_teste = '23396'
        if 'nr_pedido' in df_consolidado.columns:
            pedido_antes_dedup = len(
                df_consolidado[df_consolidado['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Pedido {pedido_teste} antes da remoção de duplicatas: {pedido_antes_dedup} registros")

            # Debug específico para datetime_coleta_dados
            if 'datetime_coleta_dados' in df_consolidado.columns and pedido_antes_dedup > 0:
                pedido_debug = df_consolidado[df_consolidado['nr_pedido']
                                              == pedido_teste]
                print(
                    f"[DEBUG] Valores de datetime_coleta_dados para pedido {pedido_teste}:")
                for idx, row in pedido_debug.iterrows():
                    print(
                        f"  - {row.get('datetime_coleta_dados', 'N/A')} | origem: {row.get('origem_processamento', 'N/A')}")

        if duplicatas_antes > 0 and coluna_data:
            # Verificar se precisa converter para datetime
            print(f"[INFO] Convertendo {coluna_data} para datetime...")
            try:
                # Tentar converter para datetime se não estiver já
                if not pd.api.types.is_datetime64_any_dtype(df_consolidado[coluna_data]):
                    if coluna_data == 'datetime_coleta_dados':
                        df_consolidado[coluna_data] = pd.to_datetime(df_consolidado[coluna_data],
                                                                     format='%d/%m/%Y %H:%M:%S',
                                                                     errors='coerce')
                    elif coluna_data in ['datetime_processamento', 'data_coleta', 'data_processamento_final']:
                        df_consolidado[coluna_data] = pd.to_datetime(df_consolidado[coluna_data], errors='coerce')
                    elif coluna_data == 'data_arquivo':
                        df_consolidado[coluna_data] = pd.to_datetime(df_consolidado[coluna_data], errors='coerce')
            except Exception as e:
                print(f"[AVISO] Erro na conversão de datetime: {e}")

            # Ordenar por data (mais recente primeiro) e manter o primeiro de cada grupo
            # PRIORIDADE ADICIONAL: Se houver campo 'coleta_especial' ou 'fonte' indicando atualização manual,
            # priorizar esses registros mesmo que a data seja igual
            print(
                f"[INFO] Ordenando por {coluna_data} (mais recente primeiro)...")
            
            # Criar coluna de prioridade para atualizações manuais
            if 'coleta_especial' in df_consolidado.columns:
                df_consolidado['_prioridade'] = df_consolidado['coleta_especial'].fillna(False).astype(int)
            elif 'fonte' in df_consolidado.columns:
                df_consolidado['_prioridade'] = df_consolidado['fonte'].str.contains('atualizacao|manual', case=False, na=False).astype(int)
            else:
                df_consolidado['_prioridade'] = 0

            # Desempate: evitar que NaT + arquivo "existente" vença staging com snapshot atualizado
            if 'origem_processamento' in df_consolidado.columns:
                df_consolidado['_rank_origem'] = (
                    df_consolidado['origem_processamento'].astype(str).str.lower() == 'novo'
                ).astype(int)
            else:
                df_consolidado['_rank_origem'] = 0

            if 'qtd_recebida' in df_consolidado.columns:
                df_consolidado['_qtd_rec_sort'] = pd.to_numeric(
                    df_consolidado['qtd_recebida'], errors='coerce')
            else:
                df_consolidado['_qtd_rec_sort'] = 0.0

            sort_cols = ['_prioridade', coluna_data, '_rank_origem', '_qtd_rec_sort']
            asc = [False, False, False, False]
            df_consolidado = df_consolidado.sort_values(
                sort_cols,
                ascending=asc,
                na_position='last',
            )
            df_consolidado = df_consolidado.drop_duplicates(
                subset=colunas_chave, keep='first')

            df_consolidado = df_consolidado.drop(
                columns=['_prioridade', '_rank_origem', '_qtd_rec_sort'],
                errors='ignore',
            )
        elif duplicatas_antes > 0 and 'origem_processamento' in df_consolidado.columns:
            # Se não há coluna de data, priorizar dados novos (staging) sobre existentes
            print("[INFO] Priorizando dados novos (staging) sobre existentes...")
            df_consolidado = df_consolidado.sort_values(
                'origem_processamento', ascending=True)  # 'existente' vem antes de 'novo'
            df_consolidado = df_consolidado.drop_duplicates(
                subset=colunas_chave, keep='last')  # Manter o último (novo)
        elif duplicatas_antes > 0:
            # Se não há coluna de data, manter o último registro (padrão)
            print("[AVISO] Sem coluna de data, mantendo último registro por posição...")
            df_consolidado = df_consolidado.drop_duplicates(
                subset=colunas_chave, keep='last')

        duplicatas_removidas = tamanho_antes - len(df_consolidado)
        print(f"[INFO] Duplicatas removidas: {duplicatas_removidas}")

        # Debug: verificar pedido teste após remoção de duplicatas
        if 'nr_pedido' in df_consolidado.columns:
            pedido_apos_dedup = len(
                df_consolidado[df_consolidado['nr_pedido'] == pedido_teste])
            print(
                f"[DEBUG] Pedido {pedido_teste} após remoção de duplicatas: {pedido_apos_dedup} registros")

        # Verificação final
        duplicatas_final = df_consolidado.duplicated(
            subset=colunas_chave).sum()
        if duplicatas_final == 0:
            print(
                f"[OK] Validação: Nenhuma duplicata restante para {colunas_chave}")
        else:
            print(
                f"[⚠️] Atenção: Ainda existem {duplicatas_final} duplicatas!")
        
        # VALIDAÇÃO DE QUANTIDADES - Verificar possíveis problemas de duplicação
        if ('codigo_pedido' in df_consolidado.columns or 'nr_pedido' in df_consolidado.columns) and 'qtd' in df_consolidado.columns:
            # Verificar pedidos específicos mencionados (usando nr_pedido que é o campo usado no BigQuery)
            pedidos_validar_silver = ['23479', '23299']
            print(f"\n[VALIDAÇÃO SILVER] Verificando quantidades de produtos...")
            
            for pedido_id in pedidos_validar_silver:
                pedido_str = str(pedido_id)
                pedido_filtrado = pd.DataFrame()
                
                # PRIORIDADE: Buscar por nr_pedido (número do pedido usado no BigQuery/view)
                if 'nr_pedido' in df_consolidado.columns:
                    try:
                        df_temp = df_consolidado.copy()
                        df_temp['nr_pedido_num'] = pd.to_numeric(df_temp['nr_pedido'], errors='coerce')
                        pedido_filtrado = df_temp[df_temp['nr_pedido_num'] == int(pedido_id)]
                    except:
                        pedido_filtrado = df_consolidado[df_consolidado['nr_pedido'].astype(str) == pedido_str]
                
                # Fallback: buscar por codigo_pedido se nr_pedido não encontrou
                if len(pedido_filtrado) == 0 and 'codigo_pedido' in df_consolidado.columns:
                    if df_consolidado['codigo_pedido'].dtype == 'object' or df_consolidado['codigo_pedido'].dtype == 'string':
                        pedido_filtrado = df_consolidado[df_consolidado['codigo_pedido'].astype(str) == pedido_str]
                    else:
                        try:
                            pedido_filtrado = df_consolidado[df_consolidado['codigo_pedido'].astype(int) == int(pedido_id)]
                        except:
                            pedido_filtrado = df_consolidado[df_consolidado['codigo_pedido'].astype(str) == pedido_str]
                
                if len(pedido_filtrado) > 0:
                    if 'produto_id' in pedido_filtrado.columns:
                        # Verificar se há produtos duplicados após remoção de duplicatas
                        produtos_duplicados = pedido_filtrado[pedido_filtrado.duplicated(subset=['produto_id'], keep=False)]
                        if len(produtos_duplicados) > 0:
                            print(f"  [ATENCAO] SILVER: Pedido {pedido_id} ainda tem produtos duplicados apos deduplicacao!")
                        else:
                            print(f"  [OK] SILVER: Pedido {pedido_id} OK - sem duplicatas de produto")
                    if 'qtd' in pedido_filtrado.columns:
                        print(f"  [INFO] SILVER: Pedido {pedido_id} - Total de itens: {len(pedido_filtrado)}, Soma qtd: {pedido_filtrado['qtd'].sum()}")

    else:
        print("[INFO] Nenhuma coluna-chave identificada para remoção de duplicatas")

    # 5. Fazer join com coluna fl_encerrado dos arquivos de encerrados e cancelados
    print("\n[PASSO 5] Fazendo join com coluna fl_encerrado...")

    # Criar DataFrame com fl_encerrado dos arquivos de encerrados e cancelados
    df_fl_encerrado = pd.DataFrame()

    if df_encerrado is not None and 'fl_encerrado' in df_encerrado.columns:
        # Selecionar apenas colunas de chave e fl_encerrado dos encerrados
        colunas_chave_join = ['codigo_pedido', 'produto_id'] if 'codigo_pedido' in df_encerrado.columns and 'produto_id' in df_encerrado.columns else [
            'codigo_pedido'] if 'codigo_pedido' in df_encerrado.columns else []

        print(
            f"[DEBUG] Colunas de chave identificadas para encerrados: {colunas_chave_join}")

        if colunas_chave_join:
            df_encerrado_join = df_encerrado[colunas_chave_join +
                                             ['fl_encerrado']].copy()
            df_fl_encerrado = pd.concat(
                [df_fl_encerrado, df_encerrado_join], ignore_index=True)
            print(
                f"[INFO] Adicionados {len(df_encerrado_join)} registros de encerrados para join")

            # Debug: verificar se o pedido 23423 está no DataFrame de join
            if 'codigo_pedido' in df_encerrado_join.columns:
                pedido_join_debug = df_encerrado_join[df_encerrado_join['codigo_pedido'] == '23423']
                print(
                    f"[DEBUG] Pedido 23423 no DataFrame de join encerrados: {len(pedido_join_debug)} registros")
                if len(pedido_join_debug) > 0:
                    print(
                        f"[DEBUG] Valores fl_encerrado do pedido 23423: {pedido_join_debug['fl_encerrado'].unique()}")
        else:
            print("[AVISO] Nenhuma coluna de chave encontrada nos dados encerrados")

    if df_cancelado is not None and 'fl_encerrado' in df_cancelado.columns:
        # Selecionar apenas colunas de chave e fl_encerrado dos cancelados
        colunas_chave_join = ['codigo_pedido', 'produto_id'] if 'codigo_pedido' in df_cancelado.columns and 'produto_id' in df_cancelado.columns else [
            'codigo_pedido'] if 'codigo_pedido' in df_cancelado.columns else []

        if colunas_chave_join:
            df_cancelado_join = df_cancelado[colunas_chave_join +
                                             ['fl_encerrado']].copy()
            df_fl_encerrado = pd.concat(
                [df_fl_encerrado, df_cancelado_join], ignore_index=True)
            print(
                f"[INFO] Adicionados {len(df_cancelado_join)} registros de cancelados para join")

    # Debug: verificar se o pedido 23423 está no DataFrame consolidado antes do join
    if 'codigo_pedido' in df_consolidado.columns:
        pedido_consolidado_debug = df_consolidado[df_consolidado['codigo_pedido'] == '23423']
        print(
            f"[DEBUG] Pedido 23423 no consolidado antes do join: {len(pedido_consolidado_debug)} registros")
    elif 'nr_pedido' in df_consolidado.columns:
        pedido_consolidado_debug = df_consolidado[df_consolidado['nr_pedido'] == '23423']
        print(
            f"[DEBUG] Pedido 23423 no consolidado antes do join (nr_pedido): {len(pedido_consolidado_debug)} registros")

    # Fazer join com o DataFrame consolidado
    if not df_fl_encerrado.empty:
        print(
            f"[DEBUG] DataFrame fl_encerrado tem {len(df_fl_encerrado)} registros")
        print(
            f"[DEBUG] Colunas do DataFrame fl_encerrado: {list(df_fl_encerrado.columns)}")

        # Debug: verificar se o pedido está no DataFrame fl_encerrado
        if 'codigo_pedido' in df_fl_encerrado.columns:
            pedido_fl_debug = df_fl_encerrado[df_fl_encerrado['codigo_pedido']
                                              == '23423']
            print(
                f"[DEBUG] Pedido 23423 no DataFrame fl_encerrado: {len(pedido_fl_debug)} registros")
        
        # CRÍTICO: Remover duplicatas do df_fl_encerrado ANTES do join para evitar multiplicação de linhas
        if 'codigo_pedido' in df_fl_encerrado.columns:
            colunas_chave_fl = ['codigo_pedido', 'produto_id'] if 'produto_id' in df_fl_encerrado.columns else ['codigo_pedido']
            tamanho_fl_antes = len(df_fl_encerrado)
            df_fl_encerrado = df_fl_encerrado.drop_duplicates(subset=colunas_chave_fl, keep='first')
            duplicatas_fl_removidas = tamanho_fl_antes - len(df_fl_encerrado)
            if duplicatas_fl_removidas > 0:
                print(f"[INFO] Removidas {duplicatas_fl_removidas} duplicatas de fl_encerrado antes do join")
        
        # Identificar colunas de chave no DataFrame consolidado
        colunas_chave_consolidado = ['codigo_pedido', 'produto_id'] if 'codigo_pedido' in df_consolidado.columns and 'produto_id' in df_consolidado.columns else [
            'codigo_pedido'] if 'codigo_pedido' in df_consolidado.columns else []

        if colunas_chave_consolidado:
            print(
                f"[INFO] Fazendo join usando chaves: {colunas_chave_consolidado}")
            
            # Verificar tamanho antes do join para detectar multiplicação
            tamanho_antes_join = len(df_consolidado)

            # Fazer left join para incluir fl_encerrado
            df_consolidado = df_consolidado.merge(
                df_fl_encerrado[colunas_chave_consolidado + ['fl_encerrado']],
                on=colunas_chave_consolidado,
                how='left',
                suffixes=('', '_join')
            )
            
            # Verificar se o join multiplicou linhas (não deveria acontecer com left join e sem duplicatas)
            tamanho_apos_join = len(df_consolidado)
            if tamanho_apos_join > tamanho_antes_join:
                print(f"[⚠️] ATENÇÃO: Join multiplicou linhas! Antes: {tamanho_antes_join}, Depois: {tamanho_apos_join}")
                print(f"     Isso pode indicar duplicatas no df_fl_encerrado que não foram removidas corretamente")
            else:
                print(f"[OK] Join realizado sem multiplicação de linhas: {tamanho_antes_join} -> {tamanho_apos_join}")

            # Se houve conflito de nomes, usar a coluna do join
            if 'fl_encerrado_join' in df_consolidado.columns:
                df_consolidado['fl_encerrado'] = df_consolidado['fl_encerrado_join']
                df_consolidado = df_consolidado.drop(
                    columns=['fl_encerrado_join'])

            print(
                f"[INFO] Join concluído. Registros com fl_encerrado: {df_consolidado['fl_encerrado'].notna().sum()}")

            # Debug: verificar se o pedido 23423 tem fl_encerrado após o join
            if 'codigo_pedido' in df_consolidado.columns:
                pedido_pos_join = df_consolidado[df_consolidado['codigo_pedido']
                                                 == '23423']
                if len(pedido_pos_join) > 0:
                    print(
                        f"[DEBUG] Pedido 23423 após join - fl_encerrado: {pedido_pos_join['fl_encerrado'].unique()}")
        else:
            print("[AVISO] Não foi possível identificar colunas de chave para join")
    else:
        print("[AVISO] Nenhum dado de fl_encerrado disponível para join")

    # Validar e preencher fl_encerrado null com 'N'
    if 'fl_encerrado' in df_consolidado.columns:
        registros_null_antes = df_consolidado['fl_encerrado'].isnull().sum()
        df_consolidado['fl_encerrado'] = df_consolidado['fl_encerrado'].fillna('N')
        
        # CORREÇÃO: Se qtd_recebida == qtd (pedido completamente recebido), marcar como encerrado
        if 'qtd' in df_consolidado.columns and 'qtd_recebida' in df_consolidado.columns:
            # Converter para numérico para comparação
            df_consolidado['qtd_num'] = pd.to_numeric(df_consolidado['qtd'], errors='coerce')
            df_consolidado['qtd_recebida_num'] = pd.to_numeric(df_consolidado['qtd_recebida'], errors='coerce')
            
            # Identificar pedidos completamente recebidos (qtd_recebida == qtd e ambos > 0)
            mask_completo = (
                (df_consolidado['qtd_recebida_num'] == df_consolidado['qtd_num']) &
                (df_consolidado['qtd_num'] > 0) &
                (df_consolidado['fl_encerrado'] == 'N')
            )
            pedidos_marcados = mask_completo.sum()
            
            if pedidos_marcados > 0:
                df_consolidado.loc[mask_completo, 'fl_encerrado'] = 'S'
                print(f"[INFO] Marcados {pedidos_marcados} pedidos como encerrados (qtd_recebida == qtd)")
            
            # Remover colunas temporárias
            df_consolidado = df_consolidado.drop(columns=['qtd_num', 'qtd_recebida_num'], errors='ignore')
        
        registros_preenchidos = df_consolidado['fl_encerrado'].isnull().sum()
        print(
            f"[INFO] Preenchidos {registros_null_antes} registros null de fl_encerrado com 'N'")
        print(
            f"[INFO] Distribuição fl_encerrado: {df_consolidado['fl_encerrado'].value_counts().to_dict()}")
    else:
        # Se não existe a coluna, criar com base na lógica de recebimento
        if 'qtd' in df_consolidado.columns and 'qtd_recebida' in df_consolidado.columns:
            df_consolidado['qtd_num'] = pd.to_numeric(df_consolidado['qtd'], errors='coerce')
            df_consolidado['qtd_recebida_num'] = pd.to_numeric(df_consolidado['qtd_recebida'], errors='coerce')
            df_consolidado['fl_encerrado'] = 'N'
            mask_completo = (
                (df_consolidado['qtd_recebida_num'] == df_consolidado['qtd_num']) &
                (df_consolidado['qtd_num'] > 0)
            )
            df_consolidado.loc[mask_completo, 'fl_encerrado'] = 'S'
            df_consolidado = df_consolidado.drop(columns=['qtd_num', 'qtd_recebida_num'], errors='ignore')
            print("[INFO] Coluna fl_encerrado criada. Pedidos completamente recebidos marcados como 'S'")
        else:
            df_consolidado['fl_encerrado'] = 'N'
            print("[INFO] Coluna fl_encerrado criada com valor 'N' para todos os registros")

    # 6. Adicionar metadados finais
    df_consolidado['data_consolidacao'] = datetime.now()
    df_consolidado['versao_consolidacao'] = 'v1.0'

    # 6. Salvar arquivo consolidado
    print(f"\n[PASSO 5] Salvando arquivo consolidado...")
    df_consolidado.to_parquet(arquivo_final_local, index=False)

    # Upload para GCS
    upload_success = upload_to_gcs(
        arquivo_final_local, GCS_BUCKET_NAME, arquivo_final_gcs)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo consolidado salvo: gs://{GCS_BUCKET_NAME}/{arquivo_final_gcs}")
        print(f"[INFO] Registros finais: {len(df_consolidado)}")
        print(f"[INFO] Colunas finais: {len(df_consolidado.columns)}")

    # 7. Limpeza de arquivos temporários
    arquivos_temp = [arquivo_existente_local,
                     arquivo_staging_local, arquivo_encerrado_local,
                     arquivo_cancelado_local, arquivo_final_local]
    for arquivo in arquivos_temp:
        if os.path.exists(arquivo):
            os.unlink(arquivo)

    # 8. Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Consolidação concluída")
    print(f"{'='*60}")
    print(
        f"- Dados existentes: {len(df_existente) if df_existente is not None else 0} registros")
    print(f"- Dados novos: {len(df_staging)} registros")
    print(
        f"- Dados encerrados: {len(df_encerrado) if df_encerrado is not None else 0} registros")
    print(
        f"- Dados cancelados: {len(df_cancelado) if df_cancelado is not None else 0} registros")
    print(f"- Total consolidado: {len(df_consolidado)} registros")
    print(f"- Colunas finais: {len(df_consolidado.columns)}")
    if 'fl_encerrado' in df_consolidado.columns:
        print(
            f"- Distribuição fl_encerrado: {df_consolidado['fl_encerrado'].value_counts().to_dict()}")
    print(f"- Arquivo final: gs://{GCS_BUCKET_NAME}/{arquivo_final_gcs}")

    print(f"\n[OK] Consolidação concluída com sucesso!")
    return True


if __name__ == "__main__":
    main()
