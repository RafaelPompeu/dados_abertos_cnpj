import os
import glob
import pandas as pd

os.makedirs('parquet_out', exist_ok=True)
coll_names = [
    'CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'IDENTIFICADOR MATRIZ/FILIAL',
    'NOME FANTASIA', 'SITUAÇÃO CADASTRAL', 'DATA SITUAÇÃO CADASTRAL',
    'MOTIVO SITUAÇÃO CADASTRAL', 'NOME DA CIDADE NO EXTERIOR', 'PAIS',
    'DATA DE INÍCIO ATIVIDADE', 'CNAE FISCAL PRINCIPAL', 'CNAE FISCAL SECUNDÁRIA',
    'TIPO DE LOGRADOURO', 'LOGRADOURO', 'NÚMERO', 'COMPLEMENTO', 'BAIRRO',
    'CEP', 'UF', 'MUNICÍPIO', 'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2',
    'DDD DO FAX', 'FAX', 'CORREIO ELETRÔNICO', 'SITUAÇÃO ESPECIAL',
    'DATA DA SITUAÇÃO ESPECIAL'
]

filenames = glob.glob('dados_cnpj/*.ESTABELE')[7:8]

for filename in filenames:
    base_name = os.path.splitext(os.path.basename(filename))[0]
    df = pd.read_csv(filename, sep=';', encoding='latin1', dtype=str, names=coll_names, header=0, on_bad_lines='skip', engine='python')
    df.to_parquet(f'parquet_out/{base_name}.parquet', index=False)
    print(f'Arquivo salvo (engine python): parquet_out/{base_name}.parquet')

