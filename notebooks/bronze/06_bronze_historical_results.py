# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Histórico de Resultados (1930-2022)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.historical_results`
# MAGIC **Fonte:** openfootball/world-cup (GitHub)
# MAGIC **Executar depois de:** script `ingestion/03_openfootball_loader.py` + upload DBFS
# MAGIC
# MAGIC Lê os JSONs de cada edição da Copa (1930-2022), explode as rodadas
# MAGIC e partidas em linhas individuais, normaliza gols por minuto e tipo.
# MAGIC
# MAGIC Esta tabela é base para:
# MAGIC   - H2H histórico entre seleções
# MAGIC   - ELO rating histórico (calculado na Silver)
# MAGIC   - Desempenho de seleções em Copas específicas

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
DBFS_HIST  = "/Volumes/lakehouse/wc_platform/files/raw/historical"
DBFS_SEEDS = "/Volumes/lakehouse/wc_platform/files/seeds"
CATALOG    = "lakehouse"
SCHEMA     = "bronze"
TABLE      = "historical_results"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

WC_YEARS = list(range(1930, 2023, 4))

# COMMAND ----------
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA     = "wc_bronze"
    FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 2. Leitura e parsing dos JSONs históricos
# MAGIC
# MAGIC Estrutura do openfootball:
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "World Cup 2022",
# MAGIC   "rounds": [
# MAGIC     {
# MAGIC       "name": "Group A",
# MAGIC       "matches": [
# MAGIC         {
# MAGIC           "num": 1,
# MAGIC           "date": "2022-11-20",
# MAGIC           "time": "19:00",
# MAGIC           "team1": {"name": "Qatar", "code": "QAT"},
# MAGIC           "team2": {"name": "Ecuador", "code": "ECU"},
# MAGIC           "score": {"ft": [0, 2], "ht": [0, 1]},
# MAGIC           "goals": [
# MAGIC             {"name": "Enner Valencia", "team": "ECU", "minute": 16},
# MAGIC             {"name": "Enner Valencia", "team": "ECU", "minute": 31}
# MAGIC           ]
# MAGIC         }
# MAGIC       ]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------
import json
from pyspark.sql import functions as F, Row
from pyspark.sql.types import *

all_rows = []

for year in WC_YEARS:
    path = f"{DBFS_HIST}/wc_{year}.json"
    try:
        content = dbutils.fs.head(path, 5_000_000)  # max 5MB por arquivo
        data    = json.loads(content)
    except Exception as e:
        print(f"⚠️  {year}: não encontrado — {e}")
        continue

    rounds = data.get("rounds", [])
    n_matches = 0

    for round_data in rounds:
        round_name = round_data.get("name", "Unknown")

        for match in round_data.get("matches", []):
            score    = match.get("score", {})
            ft_score = score.get("ft", [None, None])
            ht_score = score.get("ht", [None, None])

            # Gols detalhados
            goals = match.get("goals", [])
            goals_json = json.dumps(goals, ensure_ascii=False)

            row = {
                "year":             year,
                "round":            round_name,
                "match_num":        match.get("num"),
                "match_date":       match.get("date"),
                "match_time":       match.get("time"),
                # Times
                "home_team":        match.get("team1", {}).get("name"),
                "home_team_code":   match.get("team1", {}).get("code"),
                "away_team":        match.get("team2", {}).get("name"),
                "away_team_code":   match.get("team2", {}).get("code"),
                # Placares
                "home_score_ft":    int(ft_score[0]) if ft_score[0] is not None else None,
                "away_score_ft":    int(ft_score[1]) if ft_score[1] is not None else None,
                "home_score_ht":    int(ht_score[0]) if ht_score and ht_score[0] is not None else None,
                "away_score_ht":    int(ht_score[1]) if ht_score and ht_score[1] is not None else None,
                # Resultado
                "result":           (
                    "home" if ft_score[0] is not None and ft_score[1] is not None and ft_score[0] > ft_score[1]
                    else "away" if ft_score[0] is not None and ft_score[1] is not None and ft_score[0] < ft_score[1]
                    else "draw" if ft_score[0] is not None else None
                ),
                # Gols (JSON string — explodir na Silver)
                "goals_json":       goals_json,
                "total_goals":      len(goals),
                # Meta
                "_ingested_at":     None,  # será preenchido no Spark
            }
            all_rows.append(row)
            n_matches += 1

    print(f"✅ {year}: {n_matches} partidas processadas")

print(f"\nTotal: {len(all_rows)} partidas em {len(WC_YEARS)} Copas")

# COMMAND ----------
# MAGIC %md ## 3. Criar DataFrame e escrever Delta

# COMMAND ----------
schema = StructType([
    StructField("year",           IntegerType(), True),
    StructField("round",          StringType(),  True),
    StructField("match_num",      IntegerType(), True),
    StructField("match_date",     StringType(),  True),
    StructField("match_time",     StringType(),  True),
    StructField("home_team",      StringType(),  True),
    StructField("home_team_code", StringType(),  True),
    StructField("away_team",      StringType(),  True),
    StructField("away_team_code", StringType(),  True),
    StructField("home_score_ft",  IntegerType(), True),
    StructField("away_score_ft",  IntegerType(), True),
    StructField("home_score_ht",  IntegerType(), True),
    StructField("away_score_ht",  IntegerType(), True),
    StructField("result",         StringType(),  True),
    StructField("goals_json",     StringType(),  True),
    StructField("total_goals",    IntegerType(), True),
    StructField("_ingested_at",   StringType(),  True),
])

hist_df = spark.createDataFrame(all_rows, schema=schema) \
    .withColumn("match_date",   F.to_date("match_date", "yyyy-MM-dd")) \
    .withColumn("_ingested_at", F.current_timestamp())

print(f"Schema criado: {hist_df.count()} registros")
hist_df.printSchema()

# COMMAND ----------
(
    hist_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year")
    .saveAsTable(FULL_TABLE)
)
print(f"✅ {FULL_TABLE} gravada")

# COMMAND ----------
# MAGIC %md ## 4. Validação

# COMMAND ----------
hist = spark.table(FULL_TABLE)
print(f"Total de partidas históricas: {hist.count():,}")

print("\nPartidas por Copa:")
hist.groupBy("year") \
    .agg(
        F.count("*").alias("partidas"),
        F.sum("total_goals").alias("gols_totais"),
        F.avg("total_goals").alias("media_gols"),
    ).orderBy("year") \
    .show(30)

print("\nResultados históricos do Brasil:")
hist.filter(
    (F.col("home_team").isin(["Brazil","Brasil"])) |
    (F.col("away_team").isin(["Brazil","Brasil"]))
).select(
    "year","round","match_date",
    "home_team","home_score_ft","away_score_ft","away_team","result"
).orderBy("year","match_date") \
 .show(50, truncate=False)

# COMMAND ----------
# MAGIC %md ## 5. Também carregar o index de metadados

# COMMAND ----------
index_path = f"{DBFS_SEEDS}/world_cups_index.json"
try:
    index_df = spark.read.option("multiLine","true").json(index_path)
    index_df = index_df.withColumn("_ingested_at", F.current_timestamp())
    index_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema","true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.wc_editions_index")
    print(f"✅ wc_editions_index: {index_df.count()} edições")
    index_df.show(truncate=False)
except Exception as e:
    print(f"⚠️  Index não encontrado: {e}")
