# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — StatsBomb Matches
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.statsbomb_matches`
# MAGIC **Executar depois de:** upload dos JSONs para DBFS

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
DBFS_BASE       = "/Volumes/lakehouse/wc_platform/files/raw/statsbomb"
CATALOG         = "lakehouse"
SCHEMA          = "bronze"
TABLE           = "statsbomb_matches"
FULL_TABLE      = f"{CATALOG}.{SCHEMA}.{TABLE}"

COMPETITIONS = [
    {"slug": "wc_2022",           "competition_id": 43,  "season_id": 106, "label": "FIFA World Cup 2022"},
    {"slug": "wc_2018",           "competition_id": 43,  "season_id": 3,   "label": "FIFA World Cup 2018"},
    {"slug": "euro_2024",         "competition_id": 55,  "season_id": 282, "label": "UEFA Euro 2024"},
    {"slug": "euro_2020",         "competition_id": 55,  "season_id": 43,  "label": "UEFA Euro 2020"},
    {"slug": "copa_america_2024", "competition_id": 223, "season_id": 282, "label": "Copa América 2024"},
]

# COMMAND ----------
# MAGIC %md ## 1. Catalog / Schema

# COMMAND ----------
# Fallback: se Unity Catalog não disponível, usar hive_metastore
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"✅ Unity Catalog: {CATALOG}.{SCHEMA}")
except Exception:
    CATALOG = "hive_metastore"
    SCHEMA  = "wc_bronze"
    FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"⚠️  Fallback → {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 2. Leitura e normalização dos matches

# COMMAND ----------
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import LongType, IntegerType, DateType
from functools import reduce

all_dfs = []

for comp in COMPETITIONS:
    path = f"{DBFS_BASE}/{comp['slug']}/matches.json"
    try:
        raw = spark.read.option("multiLine", "true").json(path)
    except Exception as e:
        print(f"⚠️  {comp['label']}: não encontrado em {path}")
        continue

    # Detecta se o JSON está no formato flat (statsbombpy) ou nested (StatsBomb raw)
    if "competition_id" in raw.columns:
        df = raw.select(
            # IDs
            F.col("match_id").cast(LongType()),
            # Data e tempo
            F.col("match_date").cast(DateType()),
            F.col("kick_off"),
            F.col("match_week").cast(IntegerType()),
            F.col("match_status"),
            # Competition
            F.col("competition_id").cast(IntegerType()).alias("competition_id"),
            F.col("competition_name").alias("competition_name"),
            (F.col("competition_country_name") if "competition_country_name" in raw.columns else F.lit(None)).alias("competition_country"),
            # Season
            F.col("season_id").cast(IntegerType()).alias("season_id"),
            F.col("season").alias("season_name"),
            # Home team
            F.col("home_team_id").cast(IntegerType()).alias("home_team_id"),
            F.col("home_team").alias("home_team_name"),
            (F.col("home_team_country_name") if "home_team_country_name" in raw.columns else F.lit(None)).alias("home_team_country"),
            # Away team
            F.col("away_team_id").cast(IntegerType()).alias("away_team_id"),
            F.col("away_team").alias("away_team_name"),
            (F.col("away_team_country_name") if "away_team_country_name" in raw.columns else F.lit(None)).alias("away_team_country"),
            # Scores
            F.col("home_score").cast(IntegerType()),
            F.col("away_score").cast(IntegerType()),
            # Fase da competição
            (F.col("competition_stage_id") if "competition_stage_id" in raw.columns else F.lit(None)).cast(IntegerType()).alias("stage_id"),
            (F.col("competition_stage") if "competition_stage" in raw.columns else F.lit(None)).alias("stage_name"),
            # Estádio
            (F.col("stadium_id") if "stadium_id" in raw.columns else F.lit(None)).cast(IntegerType()).alias("stadium_id"),
            (F.col("stadium") if "stadium" in raw.columns else F.lit(None)).alias("stadium_name"),
            (F.col("stadium_country_name") if "stadium_country_name" in raw.columns else F.lit(None)).alias("stadium_country"),
            # Árbitro
            (F.col("referee_id") if "referee_id" in raw.columns else F.lit(None)).cast(IntegerType()).alias("referee_id"),
            (F.col("referee") if "referee" in raw.columns else F.lit(None)).alias("referee_name"),
            # Meta Bronze
            F.lit(comp["competition_id"]).alias("_competition_id"),
            F.lit(comp["season_id"]).alias("_season_id"),
            F.lit(comp["label"]).alias("_competition_label"),
            F.current_timestamp().alias("_ingested_at"),
        )
    else:
        df = raw.select(
            # IDs
            F.col("match_id").cast(LongType()),
            # Data e tempo
            F.col("match_date").cast(DateType()),
            F.col("kick_off"),
            F.col("match_week").cast(IntegerType()),
            F.col("match_status"),
            # Competition
            F.col("competition.competition_id").cast(IntegerType()).alias("competition_id"),
            F.col("competition.competition_name").alias("competition_name"),
            F.col("competition.country_name").alias("competition_country"),
            # Season
            F.col("season.season_id").cast(IntegerType()).alias("season_id"),
            F.col("season.season_name").alias("season_name"),
            # Home team
            F.col("home_team.home_team_id").cast(IntegerType()).alias("home_team_id"),
            F.col("home_team.home_team_name").alias("home_team_name"),
            F.col("home_team.country.name").alias("home_team_country"),
            # Away team
            F.col("away_team.away_team_id").cast(IntegerType()).alias("away_team_id"),
            F.col("away_team.away_team_name").alias("away_team_name"),
            F.col("away_team.country.name").alias("away_team_country"),
            # Scores
            F.col("home_score").cast(IntegerType()),
            F.col("away_score").cast(IntegerType()),
            # Fase da competição
            F.col("competition_stage.id").cast(IntegerType()).alias("stage_id"),
            F.col("competition_stage.name").alias("stage_name"),
            # Estádio
            F.col("stadium.id").cast(IntegerType()).alias("stadium_id"),
            F.col("stadium.name").alias("stadium_name"),
            F.col("stadium.country.name").alias("stadium_country"),
            # Árbitro
            F.col("referee.id").cast(IntegerType()).alias("referee_id"),
            F.col("referee.name").alias("referee_name"),
            # Meta Bronze
            F.lit(comp["competition_id"]).alias("_competition_id"),
            F.lit(comp["season_id"]).alias("_season_id"),
            F.lit(comp["label"]).alias("_competition_label"),
            F.current_timestamp().alias("_ingested_at"),
        )

    n = df.count()
    print(f"✅ {comp['label']}: {n} partidas")
    all_dfs.append(df)

# COMMAND ----------
matches_df = reduce(DataFrame.unionByName, all_dfs)
print(f"\nTotal consolidado: {matches_df.count()} partidas")

# COMMAND ----------
# MAGIC %md ## 3. Validação

# COMMAND ----------
print("Nulos em colunas críticas:")
for col in ["match_id","home_team_id","away_team_id","home_score","away_score","match_date"]:
    n = matches_df.filter(F.col(col).isNull()).count()
    print(f"  {col}: {'✅ 0' if n==0 else f'⚠️  {n}'}")

print("\nPartidas por competição:")
matches_df.groupBy("_competition_label").count().orderBy("count", ascending=False).show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 4. Escrita Delta

# COMMAND ----------
(
    matches_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_competition_id")
    .saveAsTable(FULL_TABLE)
)
print(f"✅ {FULL_TABLE} gravada com sucesso")
spark.sql(f"DESCRIBE DETAIL {FULL_TABLE}").select("name","numFiles","sizeInBytes").show()

# COMMAND ----------
# MAGIC %md ## 5. Amostra

# COMMAND ----------
spark.table(FULL_TABLE).select(
    "match_id","match_date","_competition_label","stage_name",
    "home_team_name","home_score","away_score","away_team_name"
).orderBy("match_date").show(20, truncate=False)
