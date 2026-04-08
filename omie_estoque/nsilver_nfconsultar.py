import pandas as pd
import json
import os
import glob
import tempfile
import shutil
from datetime import datetime, timedelta
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Configurações
ARQUIVO_EXISTENTE = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais.parquet"
ARQUIVO_STG = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais_stg.parquet"
ARQUIVO_SAIDA = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais.parquet"
BACKUP_PREFIX = "maq_prod_estoque/silver/nota_fiscal/backup/"
os.makedirs('maq_prod_estoque/silver/nota_fiscal', exist_ok=True)
os.makedirs(BACKUP_PREFIX, exist_ok=True)


def limpar_backups_antigos():
    """Remove backups com mais de 5 dias"""
    try:
        if not os.path.exists(BACKUP_PREFIX):
            return

        # Data limite (5 dias atrás)
        data_limite = datetime.now() - timedelta(days=5)

        # Lista todos os arquivos de backup
        arquivos_backup = glob.glob(os.path.join(
            BACKUP_PREFIX, "silver_notas_fiscais_backup_*.parquet"))

        arquivos_removidos = 0
        for arquivo in arquivos_backup:
            try:
                # Extrai data do nome do arquivo (formato: silver_notas_fiscais_backup_YYYYMMDD_HHMMSS.parquet)
                nome_arquivo = os.path.basename(arquivo)
                match = nome_arquivo.split('_')
                if len(match) >= 4:
                    data_str = match[3]  # YYYYMMDD
                    hora_str = match[4].split('.')[0]  # HHMMSS

                    # Converte para datetime
                    data_arquivo = datetime.strptime(
                        f"{data_str}_{hora_str}", "%Y%m%d_%H%M%S")

                    # Remove se for mais antigo que 5 dias
                    if data_arquivo < data_limite:
                        os.remove(arquivo)
                        arquivos_removidos += 1
                        print(
                            f"[BACKUP] Removido backup antigo: {nome_arquivo}")

            except Exception as e:
                print(f"[AVISO] Erro ao processar arquivo {arquivo}: {e}")

        if arquivos_removidos > 0:
            print(f"[BACKUP] {arquivos_removidos} backups antigos removidos")
        else:
            print("[BACKUP] Nenhum backup antigo encontrado para remover")

    except Exception as e:
        print(f"[ERRO] Erro ao limpar backups antigos: {e}")


def fazer_backup_arquivo_existente():
    """Faz backup do arquivo existente antes de processar"""
    try:
        # Primeiro limpa backups antigos
        limpar_backups_antigos()

        if os.path.exists(ARQUIVO_EXISTENTE):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"silver_notas_fiscais_backup_{timestamp}.parquet"
            backup_path = os.path.join(BACKUP_PREFIX, backup_name)

            shutil.copy2(ARQUIVO_EXISTENTE, backup_path)
            print(f"[BACKUP] Arquivo existente salvo em: {backup_path}")

            # Mostra quantos backups existem agora
            arquivos_backup = glob.glob(os.path.join(
                BACKUP_PREFIX, "silver_notas_fiscais_backup_*.parquet"))
            print(
                f"[BACKUP] Total de backups mantidos: {len(arquivos_backup)}")

            return True
        else:
            print("[BACKUP] Nenhum arquivo existente encontrado para backup")
            return True
    except Exception as e:
        print(f"[ERRO] Erro ao fazer backup: {e}")
        return False


def carregar_dataframe_existente():
    """Carrega o DataFrame do arquivo existente"""
    try:
        if os.path.exists(ARQUIVO_EXISTENTE):
            print(f"[INFO] Carregando arquivo existente: {ARQUIVO_EXISTENTE}")
            df_existente = pd.read_parquet(ARQUIVO_EXISTENTE)
            print(
                f"[INFO] DataFrame existente carregado: {len(df_existente)} registros")
            return df_existente
        else:
            print("[INFO] Arquivo existente não encontrado, criando DataFrame vazio")
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERRO] Erro ao carregar arquivo existente: {e}")
        return pd.DataFrame()


def carregar_dataframe_stg():
    """Carrega o DataFrame do arquivo staging"""
    try:
        if os.path.exists(ARQUIVO_STG):
            print(f"[INFO] Carregando arquivo staging: {ARQUIVO_STG}")
            df_stg = pd.read_parquet(ARQUIVO_STG)
            print(
                f"[INFO] DataFrame staging carregado: {len(df_stg)} registros")
            return df_stg
        else:
            print(f"[ERRO] Arquivo staging não encontrado: {ARQUIVO_STG}")
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERRO] Erro ao carregar arquivo staging: {e}")
        return pd.DataFrame()


def concatenar_dataframes(df_existente, df_stg):
    """Concatena os dois DataFrames"""
    try:
        if df_existente.empty and df_stg.empty:
            print("[AVISO] Ambos os DataFrames estão vazios")
            return pd.DataFrame()
        elif df_existente.empty:
            print("[INFO] Apenas DataFrame staging tem dados")
            return df_stg
        elif df_stg.empty:
            print("[INFO] Apenas DataFrame existente tem dados")
            return df_existente
        else:
            print(f"[INFO] Concatenando DataFrames:")
            print(f"  - Existente: {len(df_existente)} registros")
            print(f"  - Staging: {len(df_stg)} registros")

            df_concatenado = pd.concat(
                [df_existente, df_stg], ignore_index=True)
            print(
                f"  - Total após concatenação: {len(df_concatenado)} registros")
            return df_concatenado
    except Exception as e:
        print(f"[ERRO] Erro ao concatenar DataFrames: {e}")
        return pd.DataFrame()


def remover_duplicatas(df):
    """Remove duplicatas do DataFrame"""
    try:
        if df.empty:
            print("[AVISO] DataFrame vazio, nada para deduplicar")
            return df

        print(f"[INFO] Removendo duplicatas de {len(df)} registros...")

        # Identifica colunas que podem ser usadas para identificar duplicatas
        # Assumindo que há uma coluna única como 'nCodNF' ou similar
        colunas_chave = []

        # Procura por colunas que podem ser chaves únicas
        possiveis_chaves = ['nCodNF', 'codigo_nf',
                            'id_nota_fiscal', 'nCodNFiscal']
        for coluna in possiveis_chaves:
            if coluna in df.columns:
                colunas_chave.append(coluna)
                break

        if not colunas_chave:
            print(
                "[AVISO] Nenhuma coluna chave encontrada, usando todas as colunas para deduplicação")
            df_sem_duplicatas = df.drop_duplicates()
        else:
            print(
                f"[INFO] Usando coluna(s) chave para deduplicação: {colunas_chave}")
            df_sem_duplicatas = df.drop_duplicates(
                subset=colunas_chave, keep='last')

        duplicatas_removidas = len(df) - len(df_sem_duplicatas)
        print(f"[INFO] Duplicatas removidas: {duplicatas_removidas}")
        print(f"[INFO] Registros finais: {len(df_sem_duplicatas)}")

        return df_sem_duplicatas
    except Exception as e:
        print(f"[ERRO] Erro ao remover duplicatas: {e}")
        return df


def salvar_no_gcs(df, arquivo_local):
    """Salva o DataFrame no Google Cloud Storage"""
    try:
        if df.empty:
            print("[AVISO] DataFrame vazio, nada para salvar no GCS")
            return False

        # Define o caminho no GCS
        caminho_gcs = f"silver/nota_fiscal/{os.path.basename(arquivo_local)}"

        print(f"[GCS] Salvando arquivo no GCS: {caminho_gcs}")

        # Salva localmente primeiro
        df.to_parquet(arquivo_local, index=False)
        print(f"[INFO] Arquivo salvo localmente: {arquivo_local}")

        # Faz upload para o GCS
        sucesso = upload_to_gcs(arquivo_local, GCS_BUCKET_NAME, caminho_gcs)

        if sucesso:
            print(
                f"[OK] Arquivo salvo no GCS: gs://{GCS_BUCKET_NAME}/{caminho_gcs}")
            return True
        else:
            print(f"[ERRO] Falha ao salvar no GCS")
            return False

    except Exception as e:
        print(f"[ERRO] Erro ao salvar no GCS: {e}")
        return False


def main():
    print("[INFO] Iniciando processamento SILVER - Notas Fiscais (Consolidação)")
    print("=" * 70)

    # 1. Fazer backup do arquivo existente
    print("\n[ETAPA 1] Fazendo backup do arquivo existente...")
    if not fazer_backup_arquivo_existente():
        print("[ERRO] Falha no backup, abortando processamento")
        return

    # 2. Carregar DataFrame existente
    print("\n[ETAPA 2] Carregando arquivo existente...")
    df_existente = carregar_dataframe_existente()

    # 3. Carregar DataFrame staging
    print("\n[ETAPA 3] Carregando arquivo staging...")
    df_stg = carregar_dataframe_stg()

    if df_stg.empty:
        print("[ERRO] Arquivo staging vazio ou não encontrado, abortando processamento")
        return

    # 4. Concatenar DataFrames
    print("\n[ETAPA 4] Concatenando DataFrames...")
    df_concatenado = concatenar_dataframes(df_existente, df_stg)

    if df_concatenado.empty:
        print("[ERRO] DataFrame concatenado vazio, abortando processamento")
        return

    # 5. Remover duplicatas
    print("\n[ETAPA 5] Removendo duplicatas...")
    df_final = remover_duplicatas(df_concatenado)

    if df_final.empty:
        print("[ERRO] DataFrame final vazio após deduplicação, abortando processamento")
        return

    # 6. Salvar no GCS
    print("\n[ETAPA 6] Salvando arquivo final...")
    if salvar_no_gcs(df_final, ARQUIVO_SAIDA):
        print("\n[SUCESSO] Processamento concluído com sucesso!")
        print(f"[INFO] Arquivo final: {ARQUIVO_SAIDA}")
        print(f"[INFO] Total de registros: {len(df_final)}")

        # Mostra amostra dos dados
        print("\n[INFO] Amostra dos dados finais:")
        print(df_final.head())
    else:
        print("\n[ERRO] Falha ao salvar arquivo final")


if __name__ == "__main__":
    main()
