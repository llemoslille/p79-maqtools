import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import tempfile
import time
import re
from novo_projeto.config import APP_KEY, APP_SECRET, GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Configurações
ENDPOINT = 'https://app.omie.com.br/api/v1/produtos/pedidocompra/'
# Configuração GCS - arquivos salvos diretamente no bucket
# GCS Path: bronze/pedido_compra/

REGISTROS_POR_PAGINA = 100  # API Omie aceita no máximo 100 registros por página

# Configurações de período para consulta
# Período padrão: dia 01 do mês anterior até hoje
hoje = datetime.now()
primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
primeiro_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)

DATA_INICIO_PADRAO = primeiro_dia_mes_anterior
DATA_FIM_PADRAO = hoje


def pesquisar_pedidos_compra(pagina=1, registros_por_pagina=100, data_inicial=None, data_final=None, apenas_alterados=False):
    payload = {
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "call": "PesquisarPedCompra",
        "param": [{
            "nPagina": pagina,
            "nRegsPorPagina": registros_por_pagina,
            "lApenasImportadoApi": "F",
            "lExibirPedidosPendentes": "T",
            "lExibirPedidosFaturados": "T",
            "lExibirPedidosRecebidos": "T",
            "lExibirPedidosCancelados": "F",
            # Encerrados são coletados em script separado (nbronze_pedido_compra_encerrado.py)
            "lExibirPedidosEncerrados": "F",
            "lExibirPedidosRecParciais": "T",
            "lExibirPedidosFatParciais": "T",
            "dDataInicial": data_inicial,
            "dDataFinal": data_final,
            "lApenasAlterados": "T" if apenas_alterados else "F"  # Permite buscar apenas pedidos alterados
        }]
    }
    response = requests.post(ENDPOINT, json=payload, timeout=120)
    try:
        response.raise_for_status()
    except Exception as e:
        print(f"Erro na requisição: {e}")
        print(f"Status code: {response.status_code}")
        print(f"Resposta da API: {response.text}")

        # Verifica se é erro de API bloqueada
        if response.status_code == 425 and "API bloqueada por consumo indevido" in response.text:
            # Extrai o tempo de espera da mensagem de erro
            match = re.search(
                r'Tente novamente em (\d+) segundos', response.text)
            if match:
                wait_time = int(match.group(1))
                print(
                    f"[AVISO] API bloqueada. Aguardando {wait_time} segundos ({wait_time/60:.1f} minutos)...")
                time.sleep(wait_time + 10)  # +10 segundos de margem
                print("[INFO] Tentando novamente após o bloqueio...")
                # Tenta novamente após o sleep
                response = requests.post(ENDPOINT, json=payload, timeout=120)
                response.raise_for_status()
            else:
                raise
        else:
            raise
    return response.json()


def verificar_pedido_deveria_estar_encerrado(pedido):
    """
    Verifica se um pedido deveria estar marcado como encerrado
    Critérios:
    1. Etapa (cEtapa) = '15' OU nCodEtapa = 15
    2. Quantidade recebida = Quantidade total (100% recebido)

    Args:
        pedido: Dicionário com dados do pedido da API

    Returns:
        bool: True se deveria estar encerrado, False caso contrário
    """
    cabecalho = pedido.get('cabecalho_consulta', {})

    # Verificar etapa
    etapa = str(cabecalho.get('cEtapa', '')).strip()
    cod_etapa = cabecalho.get('nCodEtapa', None)

    etapa_encerrado = False
    if etapa == '15' or cod_etapa == 15:
        etapa_encerrado = True

    # Verificar quantidade recebida
    produtos = pedido.get('produtos_consulta', [])
    quantidade_completa = False

    if produtos and isinstance(produtos, list) and len(produtos) > 0:
        total_qtd = 0
        total_qtd_rec = 0

        for produto in produtos:
            qtd = float(produto.get('nQtde', 0) or 0)
            qtd_rec = float(produto.get('nQtdeRec', 0) or 0)
            total_qtd += qtd
            total_qtd_rec += qtd_rec

        # Verificar se está 100% recebido (com margem de erro pequena para float)
        if total_qtd > 0 and abs(total_qtd_rec - total_qtd) < 0.01:
            quantidade_completa = True

    # Pedido deveria estar encerrado se: etapa 15 OU quantidade completa
    return etapa_encerrado or quantidade_completa


def processar_pedido_compra(pedido):
    # Extrai e serializa campos complexos
    dados = {}
    cabecalho = pedido.get('cabecalho_consulta', {})
    dados.update(cabecalho)
    # Serializa campos complexos
    for campo in ['departamentos_consulta', 'frete_consulta', 'parcelas_consulta', 'produtos_consulta']:
        valor = pedido.get(campo, None)
        try:
            dados[campo] = json.dumps(valor, ensure_ascii=False)
        except Exception:
            dados[campo] = str(valor)

    # PREVENÇÃO: Verificar se o pedido deveria estar encerrado antes de marcar como 'F'
    # A API pode retornar pedidos completamente recebidos ou com etapa 15 como "normais"
    # mas eles deveriam estar marcados como encerrados
    if verificar_pedido_deveria_estar_encerrado(pedido):
        dados['fl_encerrado_cancelado'] = 'T'
    else:
        dados['fl_encerrado_cancelado'] = 'F'

    return dados


def coletar_pedidos_por_dia(data_consulta):
    """Coleta todos os pedidos de compra de um dia específico (normal + alterados)"""
    data_str = data_consulta.strftime('%d/%m/%Y')
    print(f"\n[INFO] Coletando pedidos do dia {data_str}")

    pagina = 1
    todos_pedidos = []
    pedidos_ids_coletados = set()  # Para evitar duplicatas

    try:
        # COLETA 1: Pedidos normais
        resposta = pesquisar_pedidos_compra(
            pagina=pagina,
            registros_por_pagina=REGISTROS_POR_PAGINA,
            data_inicial=data_str,
            data_final=data_str,
            apenas_alterados=False
        )

        if 'nTotalPaginas' in resposta:
            n_total_paginas = int(resposta['nTotalPaginas'])

        if 'nTotalRegistros' in resposta:
            total_registros = int(resposta['nTotalRegistros'])
            print(
                f"[INFO] Total de registros normais para {data_str}: {total_registros}")

        # Processa primeira página (normal)
        if 'pedidos_pesquisa' in resposta:
            pedidos = resposta['pedidos_pesquisa']
            if isinstance(pedidos, list):
                for pedido in pedidos:
                    dados = processar_pedido_compra(pedido)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)
            elif isinstance(pedidos, dict):
                dados = processar_pedido_compra(pedidos)
                cod_pedido = dados.get('nCodPed')
                if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                    todos_pedidos.append(dados)
                    pedidos_ids_coletados.add(cod_pedido)

        # Paginação: buscar as demais páginas (normal)
        for pagina in range(2, n_total_paginas + 1):
            print(f"[INFO] Processando página {pagina}/{n_total_paginas}")
            resposta = pesquisar_pedidos_compra(
                pagina=pagina,
                registros_por_pagina=REGISTROS_POR_PAGINA,
                data_inicial=data_str,
                data_final=data_str,
                apenas_alterados=False
            )
            if 'pedidos_pesquisa' in resposta:
                pedidos = resposta['pedidos_pesquisa']
                if isinstance(pedidos, list):
                    for pedido in pedidos:
                        dados = processar_pedido_compra(pedido)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)
                elif isinstance(pedidos, dict):
                    dados = processar_pedido_compra(pedidos)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)

        # COLETA 2: Pedidos alterados
        print(f"[INFO] Coletando pedidos alterados do dia {data_str}...")
        resposta_alterados = pesquisar_pedidos_compra(
            pagina=1,
            registros_por_pagina=REGISTROS_POR_PAGINA,
            data_inicial=data_str,
            data_final=data_str,
            apenas_alterados=True
        )

        if 'nTotalPaginas' in resposta_alterados:
            n_total_paginas_alterados = int(resposta_alterados['nTotalPaginas'])

        if 'nTotalRegistros' in resposta_alterados:
            total_registros_alterados = int(resposta_alterados['nTotalRegistros'])
            print(
                f"[INFO] Total de registros alterados para {data_str}: {total_registros_alterados}")

        # Processa primeira página (alterados)
        if 'pedidos_pesquisa' in resposta_alterados:
            pedidos = resposta_alterados['pedidos_pesquisa']
            if isinstance(pedidos, list):
                for pedido in pedidos:
                    dados = processar_pedido_compra(pedido)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)
            elif isinstance(pedidos, dict):
                dados = processar_pedido_compra(pedidos)
                cod_pedido = dados.get('nCodPed')
                if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                    todos_pedidos.append(dados)
                    pedidos_ids_coletados.add(cod_pedido)

        # Paginação: buscar as demais páginas (alterados)
        for pagina in range(2, n_total_paginas_alterados + 1):
            print(f"[INFO] Processando página alterados {pagina}/{n_total_paginas_alterados}")
            resposta_alterados = pesquisar_pedidos_compra(
                pagina=pagina,
                registros_por_pagina=REGISTROS_POR_PAGINA,
                data_inicial=data_str,
                data_final=data_str,
                apenas_alterados=True
            )
            if 'pedidos_pesquisa' in resposta_alterados:
                pedidos = resposta_alterados['pedidos_pesquisa']
                if isinstance(pedidos, list):
                    for pedido in pedidos:
                        dados = processar_pedido_compra(pedido)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)
                elif isinstance(pedidos, dict):
                    dados = processar_pedido_compra(pedidos)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)

    except Exception as e:
        print(f"[ERRO] Erro ao coletar pedidos do dia {data_str}: {e}")
        return None

    return todos_pedidos


def processar_e_salvar_dia(data_consulta, pedidos_do_dia):
    """Processa e salva os pedidos de um dia específico"""
    if not pedidos_do_dia:
        return None

    df = pd.DataFrame(pedidos_do_dia)
    print(
        f"[INFO] Processando {len(df)} pedidos do dia {data_consulta.strftime('%d/%m/%Y')}")

    # Remove colunas problemáticas para Parquet
    def coluna_invalida(serie):
        return serie.apply(lambda x: (x is None) or (isinstance(x, dict) and not x) or (isinstance(x, list) and len(x) == 0)).all()

    cols_para_remover = [col for col in df.columns if coluna_invalida(df[col])]

    for col in df.select_dtypes(include=['object']).columns:
        tipos = df[col].dropna().map(type).unique()
        if any([(t not in [str, int, float, bool]) for t in tipos]):
            cols_para_remover.append(col)

    if cols_para_remover:
        print(
            f'[INFO] Removendo {len(set(cols_para_remover))} colunas problemáticas')
        df = df.drop(columns=list(set(cols_para_remover)))

    # Adiciona coluna DATETIME e metadados
    agora = datetime.now()
    df['datetime_processamento'] = agora
    df['data_coleta'] = agora
    df['data_referencia'] = data_consulta
    df['dt_coleta_dados'] = agora.strftime('%d/%m/%Y %H:%M:%S')

    # Concatenar data_referencia + dt_coleta_dados em uma coluna datetime
    df['datetime_coleta_dados'] = df['data_referencia'].dt.strftime(
        '%d/%m/%Y') + ' ' + df['dt_coleta_dados'].str.split(' ').str[1]

    df['fonte'] = 'omie_api'
    df['camada'] = 'bronze'

    # Formato do nome do arquivo: pedido_compra_YYYYMMDD.parquet
    data_str = data_consulta.strftime('%Y%m%d')
    nome_arquivo = f'pedido_compra_{data_str}.parquet'

    # Criar arquivo temporário para upload direto ao GCS
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        caminho_temp = temp_file.name
        df.to_parquet(caminho_temp, index=False)

    # Upload direto para GCS
    gcs_path = f"bronze/pedido_compra/{nome_arquivo}"
    print(f"[GCS] Salvando diretamente no GCS...")
    upload_success = upload_to_gcs(caminho_temp, GCS_BUCKET_NAME, gcs_path)

    # Remove arquivo temporário
    os.unlink(caminho_temp)

    if upload_success:
        print(f"[GCS] [OK] Disponível em: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        print(f"    Registros: {len(df)}")
    else:
        print(f"[GCS] [ERRO] Upload falhou para {nome_arquivo}")
        return None

    return {
        'data': data_consulta.date(),
        'arquivo': nome_arquivo,
        'registros': len(df),
        'upload_gcs': upload_success,
        'colunas': list(df.columns)
    }


def gerar_lista_datas(data_inicio, data_fim):
    """Gera lista de datas entre data_inicio e data_fim"""
    datas = []
    data_atual = data_inicio
    while data_atual <= data_fim:
        datas.append(data_atual)
        data_atual += timedelta(days=1)
    return datas


def gerar_lista_meses(data_inicio, data_fim):
    """Gera lista de períodos mensais entre data_inicio e data_fim"""
    from calendar import monthrange

    meses = []
    data_atual = data_inicio.replace(day=1)

    while data_atual <= data_fim:
        # Primeiro dia do mês
        primeiro_dia = data_atual

        # Último dia do mês
        ultimo_dia_numero = monthrange(data_atual.year, data_atual.month)[1]
        ultimo_dia = data_atual.replace(day=ultimo_dia_numero)

        # Se o último dia ultrapassar data_fim, ajustar
        if ultimo_dia > data_fim:
            ultimo_dia = data_fim

        meses.append((primeiro_dia, ultimo_dia))

        # Próximo mês
        if data_atual.month == 12:
            data_atual = data_atual.replace(year=data_atual.year + 1, month=1)
        else:
            data_atual = data_atual.replace(month=data_atual.month + 1)

    return meses


def gerar_lista_anos(data_inicio, data_fim):
    """Gera lista de períodos anuais entre data_inicio e data_fim"""
    anos = []
    ano_atual = data_inicio.year
    ano_fim = data_fim.year

    while ano_atual <= ano_fim:
        # Primeiro dia do ano
        primeiro_dia = datetime(ano_atual, 1, 1)

        # Último dia do ano
        ultimo_dia = datetime(ano_atual, 12, 31)

        # Ajustar se for o primeiro ou último ano
        if ano_atual == data_inicio.year:
            primeiro_dia = data_inicio
        if ano_atual == ano_fim:
            ultimo_dia = data_fim

        anos.append((primeiro_dia, ultimo_dia))
        ano_atual += 1

    return anos


def main_anual(data_inicio=None, data_fim=None):
    """
    Função que processa pedidos de compra anualmente (1 arquivo por ano)

    Args:
        data_inicio (datetime): Data inicial para consulta
        data_fim (datetime): Data final para consulta
    """
    print("[BRONZE] Iniciando coleta ANUAL de pedidos de compra...")

    # Define período se não fornecido
    if data_inicio is None:
        data_inicio = DATA_INICIO_PADRAO
    if data_fim is None:
        data_fim = DATA_FIM_PADRAO

    print(
        f"[INFO] Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")

    # Gera lista de anos para processar
    anos = gerar_lista_anos(data_inicio, data_fim)
    print(f"[INFO] Total de anos para processar: {len(anos)}", flush=True)

    import sys
    inicio_total = datetime.now()
    resultados = []

    # Processa cada ano
    for i, (primeiro_dia, ultimo_dia) in enumerate(anos, 1):
        ano_ref = primeiro_dia.strftime('%Y')
        print(f"\n{'='*60}", flush=True)
        print(f"[PROGRESSO] Ano {i}/{len(anos)}: {ano_ref}", flush=True)
        print(f"{'='*60}", flush=True)

        # Tempo estimado
        if i > 1:
            tempo_decorrido = (datetime.now() - inicio_total).total_seconds()
            tempo_por_ano = tempo_decorrido / (i - 1)
            anos_restantes = len(anos) - i + 1
            tempo_restante = tempo_por_ano * anos_restantes
            print(
                f"[TEMPO] Decorrido: {tempo_decorrido/60:.1f}min | Restante estimado: {tempo_restante/60:.1f}min", flush=True)

        data_inicio_str = primeiro_dia.strftime('%d/%m/%Y')
        data_fim_str = ultimo_dia.strftime('%d/%m/%Y')

        print(
            f"[INFO] Período do ano: {data_inicio_str} até {data_fim_str}", flush=True)

        # Coleta pedidos do ano
        pagina = 1
        todos_pedidos = []

        try:
            # Primeira requisição
            resposta = pesquisar_pedidos_compra(
                pagina=pagina,
                registros_por_pagina=REGISTROS_POR_PAGINA,
                data_inicial=data_inicio_str,
                data_final=data_fim_str
            )

            n_total_paginas = int(resposta.get('nTotalPaginas', 1))
            total_registros = int(resposta.get('nTotalRegistros', 0))

            print(
                f"[INFO] Total de registros no ano: {total_registros}", flush=True)
            print(f"[INFO] Total de páginas: {n_total_paginas}", flush=True)

            if total_registros == 0:
                print(
                    f"[INFO] Nenhum pedido encontrado para {ano_ref}", flush=True)
                continue

            # Processa primeira página
            if 'pedidos_pesquisa' in resposta:
                pedidos = resposta['pedidos_pesquisa']
                if isinstance(pedidos, list):
                    for pedido in pedidos:
                        todos_pedidos.append(processar_pedido_compra(pedido))
                elif isinstance(pedidos, dict):
                    todos_pedidos.append(processar_pedido_compra(pedidos))

            # Processa demais páginas
            for pag in range(2, n_total_paginas + 1):
                print(
                    f"[HEARTBEAT] Processando página {pag}/{n_total_paginas}...", flush=True)
                time.sleep(0.5)

                resposta = pesquisar_pedidos_compra(
                    pagina=pag,
                    registros_por_pagina=REGISTROS_POR_PAGINA,
                    data_inicial=data_inicio_str,
                    data_final=data_fim_str
                )

                if 'pedidos_pesquisa' in resposta:
                    pedidos = resposta['pedidos_pesquisa']
                    if isinstance(pedidos, list):
                        for pedido in pedidos:
                            todos_pedidos.append(
                                processar_pedido_compra(pedido))
                    elif isinstance(pedidos, dict):
                        todos_pedidos.append(processar_pedido_compra(pedidos))

            if todos_pedidos:
                # Criar DataFrame
                df = pd.DataFrame(todos_pedidos)
                print(
                    f"[INFO] Total de pedidos coletados no ano: {len(df)}", flush=True)

                # Remove colunas problemáticas
                def coluna_invalida(serie):
                    return serie.apply(lambda x: (x is None) or (isinstance(x, dict) and not x) or (isinstance(x, list) and len(x) == 0)).all()

                cols_para_remover = [
                    col for col in df.columns if coluna_invalida(df[col])]

                for col in df.select_dtypes(include=['object']).columns:
                    tipos = df[col].dropna().map(type).unique()
                    if any([(t not in [str, int, float, bool]) for t in tipos]):
                        cols_para_remover.append(col)

                if cols_para_remover:
                    print(
                        f'[INFO] Removendo {len(set(cols_para_remover))} colunas problemáticas')
                    df = df.drop(columns=list(set(cols_para_remover)))

                # Adicionar metadados
                agora = datetime.now()
                df['datetime_processamento'] = agora
                df['data_coleta'] = agora
                df['dt_coleta_dados'] = agora.strftime('%d/%m/%Y %H:%M:%S')
                df['periodo_inicio'] = data_inicio_str
                df['periodo_fim'] = data_fim_str
                df['ano_referencia'] = ano_ref
                df['fonte'] = 'omie_api'
                df['camada'] = 'bronze'

                # Nome do arquivo anual
                nome_arquivo = f'pedido_compra_{ano_ref}.parquet'

                # Salvar temporário e fazer upload
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                    caminho_temp = temp_file.name
                    df.to_parquet(caminho_temp, index=False)

                gcs_path = f"bronze/pedido_compra/{nome_arquivo}"
                print(f"[GCS] Salvando arquivo anual...", flush=True)
                upload_success = upload_to_gcs(
                    caminho_temp, GCS_BUCKET_NAME, gcs_path)

                os.unlink(caminho_temp)

                if upload_success:
                    print(
                        f"[GCS] [OK] gs://{GCS_BUCKET_NAME}/{gcs_path}", flush=True)
                    print(
                        f"    Registros: {len(df)} | Colunas: {len(df.columns)}", flush=True)
                    resultados.append({
                        'ano': ano_ref,
                        'arquivo': nome_arquivo,
                        'registros': len(df),
                        'upload_gcs': True
                    })
                    # Resumo acumulado
                    total_acum = sum(r['registros'] for r in resultados)
                    print(
                        f"[ACUMULADO] {len(resultados)} arquivos | {total_acum} registros totais", flush=True)
                else:
                    print(f"[GCS] [ERRO] Upload falhou", flush=True)

        except Exception as e:
            print(
                f"[ERRO] Erro ao coletar ano {ano_ref}: {e}", flush=True)

    # Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Processamento Anual concluído")
    print(f"{'='*60}")
    print(
        f"- Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    print(f"- Anos processados: {len(anos)}")
    print(f"- Arquivos gerados: {len(resultados)}")

    if resultados:
        total_registros = sum(r['registros'] for r in resultados)
        print(f"- Total de registros: {total_registros}")
        print(f"\n[ARQUIVOS GERADOS]")
        for r in resultados:
            print(f"  - {r['arquivo']}: {r['registros']} registros")

    return resultados


def main_mensal(data_inicio=None, data_fim=None):
    """
    Função que processa pedidos de compra mensalmente (1 arquivo por mês)

    Args:
        data_inicio (datetime): Data inicial para consulta
        data_fim (datetime): Data final para consulta
    """
    print("[BRONZE] Iniciando coleta MENSAL de pedidos de compra...")

    # Define período se não fornecido
    if data_inicio is None:
        data_inicio = DATA_INICIO_PADRAO
    if data_fim is None:
        data_fim = DATA_FIM_PADRAO

    print(
        f"[INFO] Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")

    # Gera lista de meses para processar
    meses = gerar_lista_meses(data_inicio, data_fim)
    print(f"[INFO] Total de meses para processar: {len(meses)}", flush=True)

    import sys
    inicio_total = datetime.now()
    resultados = []

    # Processa cada mês
    for i, (primeiro_dia, ultimo_dia) in enumerate(meses, 1):
        mes_ref = primeiro_dia.strftime('%Y%m')
        print(f"\n{'='*60}", flush=True)
        print(
            f"[PROGRESSO] Mês {i}/{len(meses)}: {primeiro_dia.strftime('%m/%Y')}", flush=True)
        print(f"{'='*60}", flush=True)

        # Tempo estimado
        if i > 1:
            tempo_decorrido = (datetime.now() - inicio_total).total_seconds()
            tempo_por_mes = tempo_decorrido / (i - 1)
            meses_restantes = len(meses) - i + 1
            tempo_restante = tempo_por_mes * meses_restantes
            print(
                f"[TEMPO] Decorrido: {tempo_decorrido/60:.1f}min | Restante estimado: {tempo_restante/60:.1f}min", flush=True)

        data_inicio_str = primeiro_dia.strftime('%d/%m/%Y')
        data_fim_str = ultimo_dia.strftime('%d/%m/%Y')

        print(
            f"[INFO] Período do mês: {data_inicio_str} até {data_fim_str}", flush=True)

        # Coleta pedidos do mês (normal + alterados)
        pagina = 1
        todos_pedidos = []
        pedidos_ids_coletados = set()  # Para evitar duplicatas entre coleta normal e alterados

        try:
            # COLETA 1: Pedidos normais (lApenasAlterados=F)
            print(f"[INFO] Coletando pedidos normais...", flush=True)
            resposta = pesquisar_pedidos_compra(
                pagina=pagina,
                registros_por_pagina=REGISTROS_POR_PAGINA,
                data_inicial=data_inicio_str,
                data_final=data_fim_str,
                apenas_alterados=False
            )

            n_total_paginas = int(resposta.get('nTotalPaginas', 1))
            total_registros = int(resposta.get('nTotalRegistros', 0))

            print(
                f"[INFO] Total de registros normais no mês: {total_registros}", flush=True)
            print(f"[INFO] Total de páginas: {n_total_paginas}", flush=True)

            # Processa primeira página (normal)
            if 'pedidos_pesquisa' in resposta:
                pedidos = resposta['pedidos_pesquisa']
                if isinstance(pedidos, list):
                    for pedido in pedidos:
                        dados = processar_pedido_compra(pedido)
                        # Usar nCodPed como chave única para evitar duplicatas
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)
                elif isinstance(pedidos, dict):
                    dados = processar_pedido_compra(pedidos)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)

            # Processa demais páginas (normal)
            for pag in range(2, n_total_paginas + 1):
                print(
                    f"[HEARTBEAT] Processando página {pag}/{n_total_paginas}...", flush=True)
                time.sleep(0.5)

                resposta = pesquisar_pedidos_compra(
                    pagina=pag,
                    registros_por_pagina=REGISTROS_POR_PAGINA,
                    data_inicial=data_inicio_str,
                    data_final=data_fim_str,
                    apenas_alterados=False
                )

                if 'pedidos_pesquisa' in resposta:
                    pedidos = resposta['pedidos_pesquisa']
                    if isinstance(pedidos, list):
                        for pedido in pedidos:
                            dados = processar_pedido_compra(pedido)
                            cod_pedido = dados.get('nCodPed')
                            if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                                todos_pedidos.append(dados)
                                pedidos_ids_coletados.add(cod_pedido)
                    elif isinstance(pedidos, dict):
                        dados = processar_pedido_compra(pedidos)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)

            # COLETA 2: Pedidos alterados (lApenasAlterados=T) - IMPORTANTE para pegar pedidos atualizados
            print(f"[INFO] Coletando pedidos alterados...", flush=True)
            resposta_alterados = pesquisar_pedidos_compra(
                pagina=1,
                registros_por_pagina=REGISTROS_POR_PAGINA,
                data_inicial=data_inicio_str,
                data_final=data_fim_str,
                apenas_alterados=True
            )

            n_total_paginas_alterados = int(resposta_alterados.get('nTotalPaginas', 1))
            total_registros_alterados = int(resposta_alterados.get('nTotalRegistros', 0))

            print(
                f"[INFO] Total de registros alterados no mês: {total_registros_alterados}", flush=True)
            print(f"[INFO] Total de páginas alterados: {n_total_paginas_alterados}", flush=True)

            # Processa primeira página (alterados)
            if 'pedidos_pesquisa' in resposta_alterados:
                pedidos = resposta_alterados['pedidos_pesquisa']
                if isinstance(pedidos, list):
                    for pedido in pedidos:
                        dados = processar_pedido_compra(pedido)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)
                            print(f"[INFO] Pedido alterado coletado: nCodPed={cod_pedido}", flush=True)
                elif isinstance(pedidos, dict):
                    dados = processar_pedido_compra(pedidos)
                    cod_pedido = dados.get('nCodPed')
                    if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                        todos_pedidos.append(dados)
                        pedidos_ids_coletados.add(cod_pedido)
                        print(f"[INFO] Pedido alterado coletado: nCodPed={cod_pedido}", flush=True)

            # Processa demais páginas (alterados)
            for pag in range(2, n_total_paginas_alterados + 1):
                print(
                    f"[HEARTBEAT] Processando página alterados {pag}/{n_total_paginas_alterados}...", flush=True)
                time.sleep(0.5)

                resposta_alterados = pesquisar_pedidos_compra(
                    pagina=pag,
                    registros_por_pagina=REGISTROS_POR_PAGINA,
                    data_inicial=data_inicio_str,
                    data_final=data_fim_str,
                    apenas_alterados=True
                )

                if 'pedidos_pesquisa' in resposta_alterados:
                    pedidos = resposta_alterados['pedidos_pesquisa']
                    if isinstance(pedidos, list):
                        for pedido in pedidos:
                            dados = processar_pedido_compra(pedido)
                            cod_pedido = dados.get('nCodPed')
                            if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                                todos_pedidos.append(dados)
                                pedidos_ids_coletados.add(cod_pedido)
                    elif isinstance(pedidos, dict):
                        dados = processar_pedido_compra(pedidos)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)

            # COLETA 3: Busca adicional para o dia atual (hoje) para garantir que pedidos recém-incluídos sejam coletados
            # Isso resolve o problema de pedidos incluídos há pouco tempo que ainda não estão marcados como "alterados"
            hoje_datetime = datetime.now()
            data_hoje_str = hoje_datetime.strftime('%d/%m/%Y')
            
            # Verificar se estamos processando o mês atual (que inclui hoje)
            if ultimo_dia >= hoje_datetime.replace(hour=0, minute=0, second=0, microsecond=0):
                print(f"[INFO] Busca adicional para o dia atual ({data_hoje_str}) para garantir pedidos recém-incluídos...", flush=True)
                
                # Buscar pedidos normais de hoje (pode incluir pedidos recém-incluídos)
                resposta_hoje = pesquisar_pedidos_compra(
                    pagina=1,
                    registros_por_pagina=REGISTROS_POR_PAGINA,
                    data_inicial=data_hoje_str,
                    data_final=data_hoje_str,
                    apenas_alterados=False
                )
                
                n_total_paginas_hoje = int(resposta_hoje.get('nTotalPaginas', 1))
                total_registros_hoje = int(resposta_hoje.get('nTotalRegistros', 0))
                
                print(f"[INFO] Total de registros de hoje: {total_registros_hoje}", flush=True)
                
                # Processar primeira página (hoje)
                if 'pedidos_pesquisa' in resposta_hoje:
                    pedidos = resposta_hoje['pedidos_pesquisa']
                    if isinstance(pedidos, list):
                        for pedido in pedidos:
                            dados = processar_pedido_compra(pedido)
                            cod_pedido = dados.get('nCodPed')
                            if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                                todos_pedidos.append(dados)
                                pedidos_ids_coletados.add(cod_pedido)
                                print(f"[INFO] Pedido de hoje coletado: nCodPed={cod_pedido}", flush=True)
                    elif isinstance(pedidos, dict):
                        dados = processar_pedido_compra(pedidos)
                        cod_pedido = dados.get('nCodPed')
                        if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                            todos_pedidos.append(dados)
                            pedidos_ids_coletados.add(cod_pedido)
                            print(f"[INFO] Pedido de hoje coletado: nCodPed={cod_pedido}", flush=True)
                
                # Processar demais páginas (hoje)
                for pag in range(2, n_total_paginas_hoje + 1):
                    resposta_hoje = pesquisar_pedidos_compra(
                        pagina=pag,
                        registros_por_pagina=REGISTROS_POR_PAGINA,
                        data_inicial=data_hoje_str,
                        data_final=data_hoje_str,
                        apenas_alterados=False
                    )
                    
                    if 'pedidos_pesquisa' in resposta_hoje:
                        pedidos = resposta_hoje['pedidos_pesquisa']
                        if isinstance(pedidos, list):
                            for pedido in pedidos:
                                dados = processar_pedido_compra(pedido)
                                cod_pedido = dados.get('nCodPed')
                                if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                                    todos_pedidos.append(dados)
                                    pedidos_ids_coletados.add(cod_pedido)
                        elif isinstance(pedidos, dict):
                            dados = processar_pedido_compra(pedidos)
                            cod_pedido = dados.get('nCodPed')
                            if cod_pedido and cod_pedido not in pedidos_ids_coletados:
                                todos_pedidos.append(dados)
                                pedidos_ids_coletados.add(cod_pedido)

            if todos_pedidos:
                # Criar DataFrame
                df = pd.DataFrame(todos_pedidos)
                print(
                    f"[INFO] Total de pedidos coletados no mês: {len(df)}", flush=True)

                # Remove colunas problemáticas
                def coluna_invalida(serie):
                    return serie.apply(lambda x: (x is None) or (isinstance(x, dict) and not x) or (isinstance(x, list) and len(x) == 0)).all()

                cols_para_remover = [
                    col for col in df.columns if coluna_invalida(df[col])]

                for col in df.select_dtypes(include=['object']).columns:
                    tipos = df[col].dropna().map(type).unique()
                    if any([(t not in [str, int, float, bool]) for t in tipos]):
                        cols_para_remover.append(col)

                if cols_para_remover:
                    print(
                        f'[INFO] Removendo {len(set(cols_para_remover))} colunas problemáticas')
                    df = df.drop(columns=list(set(cols_para_remover)))

                # Adicionar metadados
                agora = datetime.now()
                df['datetime_processamento'] = agora
                df['data_coleta'] = agora
                df['dt_coleta_dados'] = agora.strftime('%d/%m/%Y %H:%M:%S')
                df['periodo_inicio'] = data_inicio_str
                df['periodo_fim'] = data_fim_str
                df['mes_referencia'] = mes_ref
                df['fonte'] = 'omie_api'
                df['camada'] = 'bronze'

                # Nome do arquivo mensal
                nome_arquivo = f'pedido_compra_{mes_ref}.parquet'

                # Salvar temporário e fazer upload
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                    caminho_temp = temp_file.name
                    df.to_parquet(caminho_temp, index=False)

                gcs_path = f"bronze/pedido_compra/{nome_arquivo}"
                print(f"[GCS] Salvando arquivo mensal...", flush=True)
                upload_success = upload_to_gcs(
                    caminho_temp, GCS_BUCKET_NAME, gcs_path)

                os.unlink(caminho_temp)

                if upload_success:
                    print(
                        f"[GCS] [OK] gs://{GCS_BUCKET_NAME}/{gcs_path}", flush=True)
                    print(
                        f"    Registros: {len(df)} | Colunas: {len(df.columns)}", flush=True)
                    resultados.append({
                        'mes': mes_ref,
                        'arquivo': nome_arquivo,
                        'registros': len(df),
                        'upload_gcs': True
                    })
                    # Resumo acumulado
                    total_acum = sum(r['registros'] for r in resultados)
                    print(
                        f"[ACUMULADO] {len(resultados)} arquivos | {total_acum} registros totais", flush=True)
                else:
                    print(f"[GCS] [ERRO] Upload falhou", flush=True)

        except Exception as e:
            print(
                f"[ERRO] Erro ao coletar mês {primeiro_dia.strftime('%m/%Y')}: {e}", flush=True)

    # Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Processamento Mensal concluído")
    print(f"{'='*60}")
    print(
        f"- Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    print(f"- Meses processados: {len(meses)}")
    print(f"- Arquivos gerados: {len(resultados)}")

    if resultados:
        total_registros = sum(r['registros'] for r in resultados)
        print(f"- Total de registros: {total_registros}")
        print(f"\n[ARQUIVOS GERADOS]")
        for r in resultados:
            print(f"  - {r['arquivo']}: {r['registros']} registros")

    return resultados


def main(data_inicio=None, data_fim=None):
    """
    Função principal que processa pedidos de compra dia por dia

    Args:
        data_inicio (datetime): Data inicial para consulta (opcional)
        data_fim (datetime): Data final para consulta (opcional)
    """
    print("[BRONZE] Iniciando coleta de pedidos de compra por dia...")

    # Define período se não fornecido
    if data_inicio is None:
        data_inicio = DATA_INICIO_PADRAO
    if data_fim is None:
        data_fim = DATA_FIM_PADRAO

    print(
        f"[INFO] Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")

    # Gera lista de datas para processar
    datas = gerar_lista_datas(data_inicio, data_fim)
    print(f"[INFO] Total de dias para processar: {len(datas)}")

    resultados = []

    # Processa cada dia
    for i, data_consulta in enumerate(datas, 1):
        print(f"\n{'='*60}")
        print(
            f"[PROGRESSO] Dia {i}/{len(datas)}: {data_consulta.strftime('%d/%m/%Y')}")
        print(f"{'='*60}")

        # Coleta pedidos do dia
        pedidos_do_dia = coletar_pedidos_por_dia(data_consulta)

        if pedidos_do_dia:
            # Processa e salva
            resultado = processar_e_salvar_dia(data_consulta, pedidos_do_dia)
            if resultado:
                resultados.append(resultado)
        else:
            print(
                f"[INFO] Nenhum dado encontrado para {data_consulta.strftime('%d/%m/%Y')}")

    # Resumo final
    print(f"\n{'='*60}")
    print(f"[RESUMO FINAL] Processamento concluído")
    print(f"{'='*60}")
    print(
        f"- Período processado: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    print(f"- Dias processados: {len(datas)}")
    print(f"- Arquivos gerados: {len(resultados)}")

    total_registros = sum(r['registros'] for r in resultados)
    uploads_sucesso = sum(1 for r in resultados if r['upload_gcs'])

    print(f"- Total de registros: {total_registros}")
    print(f"- Uploads GCS bem-sucedidos: {uploads_sucesso}/{len(resultados)}")

    if resultados:
        print(f"\n[ARQUIVOS GERADOS]")
        for resultado in resultados:
            status_gcs = "✓" if resultado['upload_gcs'] else "✗"
            print(
                f"  • {resultado['arquivo']}: {resultado['registros']} registros | GCS: {status_gcs}")

        print(f"\n[AMOSTRA] Estrutura dos dados:")
        if resultados:
            print(f"Colunas: {len(resultados[0]['colunas'])}")
            print(f"Principais: {resultados[0]['colunas'][:10]}")

    return resultados


if __name__ == "__main__":
    # MODO MENSAL: 1 arquivo por mês
    print("[CARGA MENSAL] Iniciando carga de pedidos - 1 ARQUIVO POR MÊS...")

    # Usando período padrão: dia 01 do mês anterior até hoje
    data_inicio = DATA_INICIO_PADRAO
    # Data final dinâmica: sempre até hoje para coletar pedidos mais recentes
    data_fim = DATA_FIM_PADRAO

    print(
        f"[INFO] Período: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    print(f"[INFO] Modo: 1 ARQUIVO POR MÊS")
    print(f"[INFO] Bucket: gs://{GCS_BUCKET_NAME}/bronze/pedido_compra/")
    print(f"[INFO] Nomenclatura: pedido_compra_YYYYMM.parquet")

    # Chama função de processamento mensal
    main_mensal(data_inicio, data_fim)
