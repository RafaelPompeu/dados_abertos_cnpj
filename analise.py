import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os

parquet_folder = 'parquet_out'
parquet_files = glob.glob(os.path.join(parquet_folder, '*.parquet'))
df_list = [pd.read_parquet(f) for f in parquet_files]
df = pd.concat(df_list, ignore_index=True)


cnae_to_setor = {
    # Agricultura, Pecuária, Produção Florestal, Pesca e Aqüicultura
    '01': 'Agropecuária',
    '02': 'Agropecuária',
    '03': 'Agropecuária',

    # Indústrias Extrativas
    '05': 'Indústria Extrativa',
    '06': 'Indústria Extrativa',
    '07': 'Indústria Extrativa',
    '08': 'Indústria Extrativa',
    '09': 'Indústria Extrativa',

    # Indústrias de Transformação
    '10': 'Indústria',
    '11': 'Indústria',
    '12': 'Indústria',
    '13': 'Indústria',
    '14': 'Indústria',
    '15': 'Indústria',
    '16': 'Indústria',
    '17': 'Indústria',
    '18': 'Indústria',
    '19': 'Indústria',
    '20': 'Indústria',
    '21': 'Indústria',
    '22': 'Indústria',
    '23': 'Indústria',
    '24': 'Indústria',
    '25': 'Indústria',
    '26': 'Indústria',
    '27': 'Indústria',
    '28': 'Indústria',
    '29': 'Indústria',
    '30': 'Indústria',
    '31': 'Indústria',
    '32': 'Indústria',
    '33': 'Indústria',

    # Eletricidade e Gás, Água, Esgoto, Atividades de Gestão de Resíduos
    '35': 'Infraestrutura',
    '36': 'Infraestrutura',
    '37': 'Infraestrutura',
    '38': 'Infraestrutura',
    '39': 'Infraestrutura',

    # Construção
    '41': 'Construção',
    '42': 'Construção',
    '43': 'Construção',

    # Comércio
    '45': 'Comércio',
    '46': 'Comércio',
    '47': 'Comércio',

    # Transporte, Armazenagem e Correio
    '49': 'Serviços',
    '50': 'Serviços',
    '51': 'Serviços',
    '52': 'Serviços',
    '53': 'Serviços',

    # Alojamento e Alimentação
    '55': 'Serviços',
    '56': 'Serviços',

    # Informação e Comunicação
    '58': 'Serviços',
    '59': 'Serviços',
    '60': 'Serviços',
    '61': 'Serviços',
    '62': 'Serviços',
    '63': 'Serviços',

    # Atividades Financeiras, Seguros e Serviços Relacionados
    '64': 'Serviços',
    '65': 'Serviços',
    '66': 'Serviços',

    # Atividades Imobiliárias
    '68': 'Serviços',

    # Atividades Profissionais, Científicas e Técnicas
    '69': 'Serviços',
    '70': 'Serviços',
    '71': 'Serviços',
    '72': 'Serviços',
    '73': 'Serviços',
    '74': 'Serviços',
    '75': 'Serviços',

    # Administração Pública, Defesa e Seguridade Social
    '84': 'Administração Pública',

    # Educação
    '85': 'Educação',

    # Saúde Humana e Serviços Sociais
    '86': 'Saúde',
    '87': 'Saúde',
    '88': 'Saúde',

    # Artes, Cultura, Esporte e Recreação
    '90': 'Serviços',
    '91': 'Serviços',
    '92': 'Serviços',
    '93': 'Serviços',

    # Outras atividades de serviços
    '94': 'Serviços',
    '95': 'Serviços',
    '96': 'Serviços',

    # Serviços Domésticos
    '97': 'Outros',

    # Organismos Internacionais e Outras Instituições Extraterritoriais
    '99': 'Outros'
}


def extrai_setor(cnae):
    if pd.isna(cnae):
        return 'Desconhecido'
    codigo2 = str(cnae).strip()[:2]
    return cnae_to_setor.get(codigo2, 'Outro')

df['SETOR_ECONOMICO'] = df['CNAE FISCAL PRINCIPAL'].apply(extrai_setor)
setor_dist = df['SETOR_ECONOMICO'].value_counts().reset_index()
setor_dist.columns = ['SETOR_ECONOMICO', 'Quantidade']

plt.figure(figsize=(10,6))
sns.barplot(data=setor_dist, x='Quantidade', y='SETOR_ECONOMICO', palette='viridis')
plt.title('Empresas por Setor Econômico')
plt.xlabel('Quantidade de Empresas')
plt.ylabel('Setor Econômico')
plt.tight_layout()
plt.show()

setor_uf = (
    df.groupby(['UF', 'SETOR_ECONOMICO'])
    .size()
    .reset_index(name='Quantidade')
    .pivot(index='SETOR_ECONOMICO', columns='UF', values='Quantidade')
    .fillna(0)
)

plt.figure(figsize=(14,7))
sns.heatmap(setor_uf, cmap='viridis_r', linewidths=0.5)
plt.title('Distribuição de Setores Econômicos por UF')
plt.xlabel('UF')
plt.ylabel('Setor Econômico')
plt.tight_layout()
plt.show()
