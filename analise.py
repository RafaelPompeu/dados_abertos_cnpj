import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import argparse

# Função para padronizar nomes de colunas
def padroniza_colunas(df):
    df.columns = (
        df.columns.str.upper()
        .str.replace(" ", "_")
        .str.replace("Á", "A")
        .str.replace("É", "E")
        .str.replace("Í", "I")
        .str.replace("Ó", "O")
        .str.replace("Ú", "U")
        .str.replace("Ç", "C")
        .str.replace("Ã", "A")
        .str.replace("Ê", "E")
        .str.replace("Â", "A")
        .str.replace("Ô", "O")
        .str.replace("Õ", "O")
        .str.replace("Ü", "U")
        .str.replace("Ê", "E")
    )
    return df

def main(parquet_path, uf_filtro):
    # 1. Carregar o arquivo Parquet
    df = pd.read_parquet(parquet_path)
    df = padroniza_colunas(df)
    if uf_filtro:
        if "UF" in df.columns:
            df = df[df["UF"] == uf_filtro]
        else:
            print("Coluna 'UF' não encontrada.")
            return

    # 2. Visão geral do dataset
    print("Shape:", df.shape)
    print("Colunas:", df.columns.tolist())
    print(df.dtypes)
    print(df.head())

    # 3. Verificar valores nulos
    print(df.isnull().sum().sort_values(ascending=False))

    # 4. Empresas por MUNICIPIO
    if "MUNICIPIO" in df.columns:
        plt.figure(figsize=(12,6))
        df["MUNICIPIO"].value_counts().plot(kind="bar")
        plt.title("Quantidade de Estabelecimentos por MUNICIPIO")
        plt.xlabel("MUNICIPIO")
        plt.ylabel("Quantidade")
        plt.tight_layout()
        plt.show()
    else:
        print("Coluna 'MUNICIPIO' não encontrada.")

    # 5. Situação Cadastral
    col_situacao = "SITUACAO_CADASTRAL"
    if col_situacao in df.columns:
        plt.figure(figsize=(8,4))
        df[col_situacao].value_counts().plot(kind="bar")
        plt.title("Situação Cadastral dos Estabelecimentos")
        plt.xlabel("Código da Situação")
        plt.ylabel("Quantidade")
        plt.tight_layout()
        plt.show()
    else:
        print(f"Coluna '{col_situacao}' não encontrada.")

    # 6. Identificador Matriz/Filial
    col_idmatriz = "IDENTIFICADOR_MATRIZ_FILIAL"
    if col_idmatriz in df.columns:
        plt.figure(figsize=(6,4))
        labels = df[col_idmatriz].map({"1": "Matriz", "2": "Filial"}).fillna(df[col_idmatriz])
        df[col_idmatriz].value_counts().plot(
            kind="pie", 
            autopct="%1.1f%%", 
            labels=labels.value_counts().index
        )
        plt.title("Distribuição Matriz x Filial")
        plt.ylabel("")
        plt.tight_layout()
        plt.show()
    else:
        print(f"Coluna '{col_idmatriz}' não encontrada.")

    # 7. Top 10 CNAEs principais
    col_cnae = "CNAE_FISCAL_PRINCIPAL"
    if col_cnae in df.columns:
        top_cnaes = df[col_cnae].value_counts().head(10)
        plt.figure(figsize=(10,5))
        sns.barplot(x=top_cnaes.index.astype(str), y=top_cnaes.values)
        plt.title("Top 10 CNAEs Fiscais Principais")
        plt.xlabel("CNAE")
        plt.ylabel("Quantidade")
        plt.tight_layout()
        plt.show()
    else:
        print(f"Coluna '{col_cnae}' não encontrada.")

    # 8. Estabelecimentos por ano de início de atividade
    col_data_inicio = "DATA_INICIO_ATIVIDADE"
    if col_data_inicio in df.columns:
        df[col_data_inicio] = pd.to_datetime(df[col_data_inicio], errors="coerce")
        df["ANO_INICIO"] = df[col_data_inicio].dt.year
        df["ANO_INICIO"].value_counts().sort_index().plot(kind="line", figsize=(12,6))
        plt.title("Estabelecimentos por Ano de Início da Atividade")
        plt.xlabel("Ano")
        plt.ylabel("Quantidade")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print(f"Coluna '{col_data_inicio}' não encontrada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Análise dos dados abertos do CNPJ")
    parser.add_argument("--parquet", type=str, required=False, default=os.path.join("data", "parquet_single", "part-00000-9841102f-9e8b-4271-a2a8-6ab888bcc5c2-c000.snappy.parquet"), help="Caminho do arquivo Parquet")
    parser.add_argument("--uf", type=str, required=False, default="PA", help="UF para filtrar (ex: PA)")
    args = parser.parse_args()
    main(args.parquet, args.uf)
