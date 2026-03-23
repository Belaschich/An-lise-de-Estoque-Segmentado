# %% Biblioteca
import os
import pandas as pd
import keyring
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text

#%% =========================
# 1. Criando diretório Finak
# =========================
os.makedirs("Final", exist_ok=True)
# %% fazendo a conexão com o mysql
c=keyring.get_credential(service_name="Mysql", username=None)

conexao = mysql.connector.connect(
    host="localhost",
    user=c.username,
    password=c.password,
    database="tembici"
)

cursor = conexao.cursor()
print("Conectado com sucesso!")

engine = create_engine(f"mysql+mysqlconnector://{c.username}:{c.password}@localhost/tembici")
print("Engine criada com sucesso!")


# %% criando uma base final para análise no google sheets
query = """
WITH SEGMENTACAO_ATUAL AS (
SELECT  * FROM 
(SELECT
    CODIGO,
    PROJETO,
    SEGMENTACAO,
    DATA_INGESTAO,
    ROW_NUMBER() OVER (
    PARTITION BY CODIGO, PROJETO
    ORDER BY DATA_INGESTAO DESC
  ) RN
  FROM SEGMENTACAO
  
) as T
where 
T.RN=1),


ESTOQUE_SEGMENTADO AS (
  SELECT
    e.DATA,
    e.PROJETO,
    e.CODIGO,
    COALESCE(s.SEGMENTACAO, 'SEM_SEGMENTACAO') AS SEGMENTACAO,
    SUM(e.QUANTIDADE) AS QUANTIDADE
  FROM ESTOQUE e
  LEFT JOIN SEGMENTACAO_ATUAL s
    ON e.CODIGO = s.CODIGO
   AND e.PROJETO = s.PROJETO
  GROUP BY
    e.DATA,
    e.PROJETO,
    e.CODIGO,
    s.SEGMENTACAO
)

SELECT
  DATA,
  PROJETO,
  SEGMENTACAO,
  COUNT(DISTINCT CODIGO) AS QTD_CODIGOS,
  SUM(QUANTIDADE) AS ESTOQUE_TOTAL
FROM ESTOQUE_SEGMENTADO
GROUP BY
  DATA,
  PROJETO,
  SEGMENTACAO
ORDER BY
  DATA,
  PROJETO,
  ESTOQUE_TOTAL DESC;
"""


df_final = pd.read_sql(text(query), engine)
df_final.to_excel("Final/base_final.xlsx", index=False)
print(df_final.head())
# %%
