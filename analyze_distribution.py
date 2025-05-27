import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os
import json

parquet_folder = 'parquet_out'
parquet_files = glob.glob(os.path.join(parquet_folder, '*.parquet'))

df_list = [pd.read_parquet(f) for f in parquet_files]
df = pd.concat(df_list, ignore_index=True)

with open('municipios_depara.json', 'r', encoding='utf-8') as f:
    municipio_depara = json.load(f)

companies_by_municipality = df.groupby('MUNICÍPIO').size().reset_index(name='Quantity')
companies_by_municipality['MUNICÍPIO_NOME'] = companies_by_municipality['MUNICÍPIO'].apply(lambda x: municipio_depara.get(str(x).zfill(4), str(x)))
top_municipalities = companies_by_municipality.sort_values(by='Quantity', ascending=False).head(10)

plt.figure(figsize=(10,6))
sns.barplot(data=top_municipalities, x='Quantity', y='MUNICÍPIO_NOME', palette='viridis')
plt.title('Top 10 Municípios com mais Empresas')
plt.xlabel('Quantidade de Empresas')
plt.ylabel('Município')
plt.tight_layout()
plt.show()

companies_by_uf = df.groupby('UF').size().reset_index(name='Quantity')
top_ufs = companies_by_uf.sort_values(by='Quantity', ascending=False).head(10)

plt.figure(figsize=(10,6))
sns.barplot(data=top_ufs, x='Quantity', y='UF', palette='viridis')
plt.title('Top 10 UFs com mais Empresas')
plt.xlabel('Quantidade de Empresas')
plt.ylabel('UF')
plt.tight_layout()
plt.show()

