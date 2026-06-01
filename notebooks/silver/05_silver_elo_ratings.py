# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — ELO Ratings Históricos
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver
# MAGIC **Tabela:** `worldcup.silver.elo_ratings`
# MAGIC **Fonte:** `worldcup.bronze.historical_results`
# MAGIC **Executar depois de:** 06_bronze_historical_results
# MAGIC
# MAGIC Calcula ELO rating para cada seleção após cada Copa do Mundo (1930-2022).
# MAGIC O ELO de Copa 2026 é calculado a partir do estado após a Copa 2022
# MAGIC mais as competições recentes (Euro 2024, Copa América 2024).
# MAGIC
# MAGIC **Por que ELO:**
# MAGIC É a feature individual mais preditiva para resultado de partidas
# MAGIC internacionais — supera FIFA ranking em backtest histórico.
# MAGIC ELO diferencial entre times = feature #1 no match predictor.
# MAGIC
# MAGIC **Fórmula ELO de Copa (adaptada de Elo World Football):**
# MAGIC   Elo_new = Elo_old + K × (W - We)
# MAGIC   K = 60 (Copa do Mundo, partida eliminatória)
# MAGIC   K = 50 (Copa do Mundo, fase de grupos)
# MAGIC   We = 1 / (1 + 10^((Elo_opp - Elo_team)/400))

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_B   = "bronze"
TABLE      = "elo_ratings"
FULL_TABLE = f"{CATALOG}.{SCHEMA_S}.{TABLE}"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_S}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_S   = "wc_silver"
    SCHEMA_B   = "wc_bronze"
    FULL_TABLE = f"{CATALOG}.{SCHEMA_S}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_S}")

# COMMAND ----------
# MAGIC %md ## 1. Leitura do histórico

# COMMAND ----------
from pyspark.sql import functions as F
import math

hist = spark.table(f"{CATALOG}.{SCHEMA_B}.historical_results") \
    .filter(F.col("home_score_ft").isNotNull()) \
    .orderBy("year","match_date","match_num") \
    .toPandas()

print(f"Partidas históricas: {len(hist)}")
print(hist[["year","home_team","home_score_ft","away_score_ft","away_team"]].head(10))

# COMMAND ----------
# MAGIC %md ## 2. Cálculo iterativo do ELO

# COMMAND ----------
import pandas as pd

# ELO inicial = 1000 para todas as seleções
elo = {}

# K-factor por fase
def get_k(round_name: str) -> int:
    round_lower = round_name.lower()
    if any(x in round_lower for x in ["final","semi","third","quarter"]):
        return 60
    elif any(x in round_lower for x in ["round of 16","round of 32","last 16"]):
        return 55
    else:
        return 50   # grupos

def expected_score(elo_a: float, elo_b: float) -> float:
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))

def get_result(home_score: int, away_score: int) -> tuple:
    """Retorna (resultado_home, resultado_away): 1=win, 0.5=draw, 0=loss"""
    if home_score > away_score:
        return 1.0, 0.0
    elif home_score == away_score:
        return 0.5, 0.5
    else:
        return 0.0, 1.0

# Histórico de ratings para cada partida
records = []

for _, row in hist.iterrows():
    home = row["home_team"]
    away = row["away_team"]

    if home not in elo: elo[home] = 1000
    if away not in elo: elo[away] = 1000

    elo_home = elo[home]
    elo_away = elo[away]

    we_home = expected_score(elo_home, elo_away)
    we_away = 1 - we_home

    w_home, w_away = get_result(
        int(row["home_score_ft"]),
        int(row["away_score_ft"])
    )

    k = get_k(str(row.get("round", "")))

    new_elo_home = elo_home + k * (w_home - we_home)
    new_elo_away = elo_away + k * (w_away - we_away)

    # Registrar estado ANTES e DEPOIS da partida
    records.append({
        "year":             int(row["year"]),
        "round":            str(row.get("round","")),
        "match_date":       str(row.get("match_date","")),
        "home_team":        home,
        "away_team":        away,
        "home_score":       int(row["home_score_ft"]),
        "away_score":       int(row["away_score_ft"]),
        "elo_home_before":  round(elo_home, 1),
        "elo_away_before":  round(elo_away, 1),
        "elo_home_after":   round(new_elo_home, 1),
        "elo_away_after":   round(new_elo_away, 1),
        "elo_diff":         round(elo_home - elo_away, 1),
        "we_home":          round(we_home, 4),
        "we_away":          round(we_away, 4),
        "k_factor":         k,
    })

    elo[home] = new_elo_home
    elo[away] = new_elo_away

print(f"Registros de ELO calculados: {len(records)}")
print(f"\nELO atual (após Copa 2022) — Top 15:")
elo_sorted = sorted(elo.items(), key=lambda x: x[1], reverse=True)
for team, rating in elo_sorted[:15]:
    print(f"  {team:25s} {rating:.1f}")

# COMMAND ----------
# MAGIC %md ## 3. Snapshot final de ELO por seleção (para feature store)

# COMMAND ----------
# Estado final do ELO após toda a história
elo_snapshot = pd.DataFrame([
    {"team_name": team, "elo_rating": round(rating, 1), "elo_source": "historical_1930_2022"}
    for team, rating in elo.items()
]).sort_values("elo_rating", ascending=False)

print(f"Seleções com ELO calculado: {len(elo_snapshot)}")

# COMMAND ----------
# MAGIC %md ## 4. Escrever tabelas Delta

# COMMAND ----------
from pyspark.sql.types import *

# Tabela de partidas com ELO
elo_matches_schema = StructType([
    StructField("year",             IntegerType(), True),
    StructField("round",            StringType(),  True),
    StructField("match_date",       StringType(),  True),
    StructField("home_team",        StringType(),  True),
    StructField("away_team",        StringType(),  True),
    StructField("home_score",       IntegerType(), True),
    StructField("away_score",       IntegerType(), True),
    StructField("elo_home_before",  DoubleType(),  True),
    StructField("elo_away_before",  DoubleType(),  True),
    StructField("elo_home_after",   DoubleType(),  True),
    StructField("elo_away_after",   DoubleType(),  True),
    StructField("elo_diff",         DoubleType(),  True),
    StructField("we_home",          DoubleType(),  True),
    StructField("we_away",          DoubleType(),  True),
    StructField("k_factor",         IntegerType(), True),
])

elo_matches_df = spark.createDataFrame(records, schema=elo_matches_schema) \
    .withColumn("_processed_at", F.current_timestamp())

elo_matches_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("year") \
    .saveAsTable(FULL_TABLE)
print(f"✅ {FULL_TABLE}: {elo_matches_df.count()} partidas com ELO")

# Snapshot atual (para join na Gold/ML)
elo_snapshot_df = spark.createDataFrame(elo_snapshot) \
    .withColumn("_processed_at", F.current_timestamp())

elo_snapshot_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_S}.elo_current_snapshot")
print(f"✅ elo_current_snapshot: {elo_snapshot_df.count()} seleções")

# COMMAND ----------
# MAGIC %md ## 5. Validação

# COMMAND ----------
elo_df = spark.table(FULL_TABLE)

print("\n📈 Evolução do ELO do Brasil por Copa:")
elo_df.filter(
    (F.col("home_team").isin(["Brazil","Brasil"])) |
    (F.col("away_team").isin(["Brazil","Brasil"]))
).select(
    "year","round",
    "home_team","home_score","away_score","away_team",
    F.when(F.col("home_team").isin(["Brazil","Brasil"]),
        F.col("elo_home_after")
    ).otherwise(F.col("elo_away_after")).alias("brazil_elo_after"),
    F.col("elo_diff"),
).orderBy("year","match_date") \
 .show(30, truncate=False)

print("\n🌍 Top 20 seleções por ELO (após Copa 2022):")
spark.table(f"{CATALOG}.{SCHEMA_S}.elo_current_snapshot") \
    .orderBy("elo_rating", ascending=False) \
    .show(20, truncate=False)
