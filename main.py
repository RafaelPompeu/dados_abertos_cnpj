#!/usr/bin/env python3
"""
Script unificado para baixar, descompactar, ler e salvar em Parquet os dados abertos do CNPJ.
"""

import os
import glob
import zipfile
import shutil
import requests
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-04/Estabelecimentos{}.zip"
RAW_DATA_DIR = "raw_data"
UNZIP_DIR = os.path.join(RAW_DATA_DIR, "unzipped")
PARQUET_DIR = "parquet_data"

def download_file(idx):
    url = BASE_URL.format(idx)
    local_filename = os.path.join(RAW_DATA_DIR, f"Estabelecimentos{idx}.zip")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Baixado: {local_filename}")
    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")

def download_all(max_idx=10, max_workers=5):
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    indices = range(max_idx)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(download_file, indices)

def unzip_all():
    os.makedirs(UNZIP_DIR, exist_ok=True)
    zip_files = glob.glob(os.path.join(RAW_DATA_DIR, "Estabelecimentos*.zip"))
    for zip_path in zip_files:
        print(f"Descompactando: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                temp_extract_path = zip_ref.extract(file, UNZIP_DIR)
                final_path = os.path.join(UNZIP_DIR, os.path.basename(file))
                if not os.path.exists(final_path):
                    shutil.move(temp_extract_path, final_path)
                elif temp_extract_path != final_path:
                    os.remove(temp_extract_path)

def read_and_save_parquet():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    txt_files = glob.glob(os.path.join(UNZIP_DIR, "*.txt"))
    for txt_file in txt_files:
        print(f"Lendo: {txt_file}")
        # Ajuste o encoding e separador conforme necessário
        df = pd.read_csv(txt_file, sep=';', encoding='latin1', dtype=str, low_memory=False)
        parquet_file = os.path.join(PARQUET_DIR, os.path.basename(txt_file).replace('.txt', '.parquet'))
        df.to_parquet(parquet_file, index=False)
        print(f"Salvo: {parquet_file}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline CNPJ: baixar, descompactar, ler e salvar em Parquet.")
    parser.add_argument('--download', action='store_true', help='Baixar arquivos zip')
    parser.add_argument('--unzip', action='store_true', help='Descompactar arquivos zip')
    parser.add_argument('--parquet', action='store_true', help='Ler arquivos txt e salvar em Parquet')
    parser.add_argument('--all', action='store_true', help='Executar todas as etapas')
    parser.add_argument('--max-idx', type=int, default=10, help='Número de arquivos para baixar (default: 10)')
    args = parser.parse_args()

    if args.all or args.download:
        download_all(max_idx=args.max_idx)
    if args.all or args.unzip:
        unzip_all()
    if args.all or args.parquet:
        read_and_save_parquet()
