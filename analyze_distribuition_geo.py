import pandas as pd
import plotly.express as px
import glob
import os
import json

parquet_folder = 'parquet_out'
parquet_files = glob.glob(os.path.join(parquet_folder, '*.parquet'))

df_list = [pd.read_parquet(f) for f in parquet_files]
df = pd.concat(df_list, ignore_index=True)
df_uf = df.groupby('UF').size().reset_index(name='qtd_estabelecimentos')

geojson_path = 'brazil-states.geojson'
with open(geojson_path, encoding='utf-8') as f:
    br_states = json.load(f)

fig = px.choropleth(
    df_uf,
    geojson=br_states,
    locations='UF',
    featureidkey='properties.sigla',
    color='qtd_estabelecimentos',
    color_continuous_scale=px.colors.sequential.Viridis[::-1],
    labels={'qtd_estabelecimentos': 'Qtd. Estabelecimentos'},
    title='Estabelecimentos por UF'
)
fig.update_geos(fitbounds="locations", visible=False)
fig.show()
