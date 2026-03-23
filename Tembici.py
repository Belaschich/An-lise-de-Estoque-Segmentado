# %% Bibliotecas
import os
import pandas as pd
import keyring
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text

#%% =========================
# 1. Criando diretórios
# =========================
os.makedirs("data/bronze", exist_ok=True)
os.makedirs("data/silver", exist_ok=True)
os.makedirs("data/gold", exist_ok=True)

#%% =========================
# 2. Leitura da camada RAW de estoque e segmentação
# =========================
arquivo_estoque = "Case Analista de Planejamento Operacional - Tabela_ estoque.csv"
df_estoque = pd.read_csv(arquivo_estoque)

arquivo_segmentacao = "Case Analista de Planejamento Operacional - Tabela_ segmentação.csv"
df_segmentacao = pd.read_csv(arquivo_segmentacao)
#%% =========================
# 3. Camada BRONZE
# =========================
# Objetivo: armazenar o dado bruto, praticamente sem tratamento

df_bronze_1 = df_estoque.copy()
df_bronze_2 = df_segmentacao.copy()

#%% opcional: adicionar metadata
df_bronze_1["date"] = pd.to_datetime(df_bronze_1["date"])
df_bronze_2["ingestion_date"] = pd.to_datetime(df_bronze_2["ingestion_date"])
#%% salvar bronze
df_bronze_1.to_csv("data/bronze/Estoque_Bronze.csv", index=False, encoding="utf-8-sig")
df_bronze_2.to_csv("data/bronze/Segmentacao_Bronze.csv", index=False, encoding="utf-8-sig")

print("Camada Bronze criada com sucesso.")

#%% =========================
# 4. Camada SILVER
# =========================

# Objetivo: limpar, tipar, remover inconsistências e padronizar

df_silver_1 = df_bronze_1.copy()
df_silver_2 = df_bronze_2.copy()

#%% converter tipos
#df_silver_1["date"] = pd.to_datetime(df_silver_1["date"]).dt.normalize()
#df_silver_1["saldo_em_estoque"] = pd.to_numeric(df_silver_1["saldo_em_estoque"], errors="coerce").astype(float)

#df_silver_2["ingestion_date"] = pd.to_datetime(df_silver_2["ingestion_date"]).dt.normalize()
#%% remover duplicados
df_silver_1 = df_silver_1.drop_duplicates()
df_silver_2 = df_silver_2.drop_duplicates()

#%% remover linhas com dados críticos nulos
df_silver_1 = df_silver_1.dropna(subset=["date", "codigo", "saldo_em_estoque", "projeto"])
df_silver_2 = df_silver_2.dropna(subset=["codigo", "segmentacao", "projeto", "ingestion_date"])

#%% tratar valores inválidos
df_silver_1 = df_silver_1[df_silver_1["saldo_em_estoque"] > 0]

#%% padronizar textos
colunas_texto_1 = ["codigo", "projeto"]
for col in colunas_texto_1:
    df_silver_1[col] = df_silver_1[col].astype(str).str.strip().str.title()

colunas_texto_2 = ["codigo", "segmentacao", "projeto"]
for col in colunas_texto_2:
    df_silver_2[col] = df_silver_2[col].astype(str).str.strip().str.title()


#%% salvar silver
df_silver_1.to_csv("data/silver/estoque_silver.csv", index=False, encoding="utf-8-sig")
df_silver_2.to_csv("data/silver/segmentacao_silver.csv", index=False, encoding="utf-8-sig")


print("Camada Silver criada com sucesso.")

#%% =========================
# 5. Camada GOLD
# =========================

# Objetivo: criar visão analítica e regras de negócio

df_gold_1 = df_silver_1.copy()
df_gold_2 = df_silver_2.copy()

#%% converter tipos
#df_gold_1["date"] = pd.to_datetime(df_gold_1["date"]).dt.normalize()
#df_gold_2["ingestion_date"] = pd.to_datetime(df_gold_2["ingestion_date"]).dt.normalize()
#%% criar métricas
df_gold_1["ano"] = df_gold_1["date"].dt.year
df_gold_1["mes"] = df_gold_1["date"].dt.month
df_gold_1["mes_nome"] = df_gold_1["date"].dt.month_name()

df_gold_2["ano"] = df_gold_2["ingestion_date"].dt.year
df_gold_2["mes"] = df_gold_2["ingestion_date"].dt.month
df_gold_2["mes_nome"] = df_gold_2["ingestion_date"].dt.month_name()


#%% tabela para criar o sql
df_gold_1.to_csv("data/gold/estoque_gold.csv", index=False, encoding="utf-8-sig")
df_gold_2.to_csv("data/gold/segmentacao_gold.csv", index=False, encoding="utf-8-sig")


print("Camada Gold criada com sucesso.")

#%% Ajustando as colunas da Golden_1 e Golden_2 para melhor inserção da tabela

df_gold_1 = df_gold_1.rename(columns={
    "date": "DATA",
    "codigo": "CODIGO",
    "saldo_em_estoque": "QUANTIDADE",
    "projeto": "PROJETO",
    "ano":"ANO",
    "mes":"MES",
    "mes_nome":"MES_NOME"
})

df_gold_2 = df_gold_2.rename(columns={
    "codigo": "CODIGO",
    "segmentacao": "SEGMENTACAO",
    "projeto": "PROJETO",
    "ingestion_date": "DATA_INGESTAO",
    "ano":"ANO",
    "mes":"MES",
    "mes_nome":"MES_NOME"    
    
})
# %% fazendo a conexão com o mysql
c=keyring.get_credential(service_name="Mysql", username=None)

conexao = mysql.connector.connect(
    host="localhost",
    user=c.username,
    password=c.password
    #database="tembici"
)

cursor = conexao.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS tembici")
print("Database garantido!")

conexao.database = "tembici"
print("Conectado com sucesso!")

engine = create_engine(f"mysql+mysqlconnector://{c.username}:{c.password}@localhost/tembici")
print("Engine criada com sucesso!")

# %% Criando as Tabelas Caso não exista
estoque = """
CREATE TABLE IF NOT EXISTS ESTOQUE (
    ID BIGINT AUTO_INCREMENT PRIMARY KEY,    
    DATA DATE NOT NULL,
    CODIGO VARCHAR(50),
    QUANTIDADE DECIMAL(15,2),
    PROJETO VARCHAR(50),
    ANO VARCHAR(4),
    MES VARCHAR(2),
    MES_NOME VARCHAR(50),
    INDEX idx_data (DATA),
    INDEX idx_codigo (CODIGO)
);
"""

segmentacao = """
CREATE TABLE IF NOT EXISTS SEGMENTACAO (
    ID BIGINT AUTO_INCREMENT PRIMARY KEY,    
    CODIGO VARCHAR(50),
    SEGMENTACAO VARCHAR(50),
    PROJETO VARCHAR(50),
    DATA_INGESTAO DATE NOT NULL,
    ANO VARCHAR(4),
    MES VARCHAR(2),
    MES_NOME VARCHAR(50),
    INDEX idx_data (DATA_INGESTAO),
    INDEX idx_codigo (CODIGO)
);
"""

#%% Criar tabela se não existir
with engine.connect() as conn:
    conn.execute(text(estoque))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text(segmentacao))
    conn.commit()

#%% Limpa os dados antes de inserir novamente
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE ESTOQUE"))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE SEGMENTACAO"))
    conn.commit()

#%%  Inserindo dados
df_gold_1.to_sql(
    "ESTOQUE",
    con=engine,
    if_exists="append",
    index=False
)

df_gold_2.to_sql(
    "SEGMENTACAO",
    con=engine,
    if_exists="append",
    index=False
)
print("Tabela criada e dados inseridos com sucesso!")

# %%
