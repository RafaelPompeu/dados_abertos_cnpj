import os
import requests
from zipfile import ZipFile

# Configurações
base_url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-05/Estabelecimentos{}.zip'
dest_folder_raw = 'dados_cnpj_raw'
dest_folder = 'dados_cnpj'

os.makedirs(dest_folder_raw, exist_ok=True)

os.makedirs(dest_folder, exist_ok=True)

for i in range(10)[7:]:
    filename = f'Empresas{i}.zip'
    zip_path = os.path.join(dest_folder_raw, filename)
    url = base_url.format(i)
    
    if not os.path.exists(zip_path):
        print(f'Baixando {filename}...')
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f'{filename} baixado com sucesso.')
    else:
        print(f'{filename} já existe. Pulando download.')

    print(f'Descompactando {filename}...')
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(dest_folder)
    print(f'{filename} descompactado.\n')

print('Processo finalizado!')
