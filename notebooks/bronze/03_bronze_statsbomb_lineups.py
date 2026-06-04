# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — StatsBomb Lineups
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.statsbomb_lineups`
# MAGIC **Executar depois de:** 01_bronze_statsbomb_matches
# MAGIC
# MAGIC Lê lineups/{match_id}.json — escalações confirmadas com posições,
# MAGIC substituições por minuto e cartões. Explode o array de jogadores
# MAGIC em linhas individuais (1 linha = 1 jogador por partida).

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

DBFS_BASE  = "/Volumes/lakehouse/wc_platform/files/raw/statsbomb"
CATALOG    = "lakehouse"
SCHEMA     = "bronze"
TABLE      = "statsbomb_lineups"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

COMPETITIONS = [
    {"slug": "wc_2022",           "competition_id": 43,  "season_id": 106, "label": "FIFA World Cup 2022"},
    {"slug": "wc_2018",           "competition_id": 43,  "season_id": 3,   "label": "FIFA World Cup 2018"},
    {"slug": "euro_2024",         "competition_id": 55,  "season_id": 282, "label": "UEFA Euro 2024"},
    {"slug": "euro_2020",         "competition_id": 55,  "season_id": 43,  "label": "UEFA Euro 2020"},
    {"slug": "copa_america_2024", "competition_id": 223, "season_id": 282, "label": "Copa América 2024"},
]

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

# MAGIC %md ## 2. Leitura e explosão dos lineups

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType

spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

for comp in COMPETITIONS:
    lineups_path = f"{DBFS_BASE}/{comp['slug']}/lineups"

    try:
        files = dbutils.fs.ls(lineups_path)
    except Exception:
        print(f"⚠️  {comp['label']}: lineups não encontrados")
        continue

    print(f"\n📂 {comp['label']}: {len(files)} arquivos")

    # Cada arquivo JSON tem estrutura:
    # [ { "team_id": X, "team_name": "...", "lineup": [ {player}, ... ] } ]
    raw = (
        spark.read
        .option("multiLine", "true")
        .json(lineups_path)
    )

    # O arquivo de lineup tem colunas nomeadas por time (ex: "Brazil", "Argentina")
    # Cada coluna é um array de jogadores
    # Empilhar todas as colunas de times em linhas
    from pyspark.sql.types import StringType
    
    # Extrair match_id do nome do arquivo (usar _metadata.file_path no Unity Catalog)
    match_id_col = F.regexp_extract(F.col("_metadata.file_path"), r"/(\d+)\.json$", 1).cast(LongType())
    
    # Stack todas as colunas de times (cada coluna é um time diferente)
    # Gerar (team_name, players_array) para cada time
    team_cols = [col for col in raw.columns]
    
    # Para cada time, criar um DataFrame separado e depois union
    dfs = []
    for team_name in team_cols:
        team_df = raw.select(
            match_id_col.alias("match_id"),
            F.lit(team_name).alias("team_name"),
            F.explode(F.col(team_name)).alias("player")
        )
        dfs.append(team_df)
    
    teams_exploded = dfs[0]
    for df in dfs[1:]:
        teams_exploded = teams_exploded.union(df)

    # Explodir campos do jogador
    df = teams_exploded.select(
        "match_id",
        "team_name",
        # Jogador
        F.col("player.player_id").cast(IntegerType()).alias("player_id"),
        F.col("player.player_name").alias("player_name"),
        F.col("player.player_nickname").alias("player_nickname"),
        F.col("player.jersey_number").cast(IntegerType()).alias("jersey_number"),
        F.col("player.country").alias("country_name"),
        # Posição inicial (primeiro elemento do array positions) - usar expr com get para segurança
        F.expr("get(player.positions, 0).position_id").cast(IntegerType()).alias("position_id"),
        F.expr("get(player.positions, 0).position").alias("position_name"),
        F.expr("get(player.positions, 0).from").alias("position_from"),
        F.expr("get(player.positions, 0).to").alias("position_to"),
        # Cartões
        F.size(F.col("player.cards")).alias("n_cards"),
        # Posições completas (array para Silver)
        F.col("player.positions").alias("positions_raw"),
        F.col("player.cards").alias("cards_raw"),
        # Meta
        F.lit(comp["competition_id"]).cast(IntegerType()).alias("_competition_id"),
        F.lit(comp["season_id"]).cast(IntegerType()).alias("_season_id"),
        F.lit(comp["label"]).alias("_competition_label"),
        F.current_timestamp().alias("_ingested_at"),
    )

    n = df.count()
    print(f"   Registros (jogador x partida): {n:,}")

    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .partitionBy("_competition_id") \
      .saveAsTable(FULL_TABLE)

    print(f"   ✅ {comp['label']} escrita")

# COMMAND ----------

# MAGIC %md ## 3. Validação

# COMMAND ----------

lineups = spark.table(FULL_TABLE)
print(f"Total registros: {lineups.count():,}")

print("\nJogadores únicos por competição:")
lineups.groupBy("_competition_label") \
    .agg(
        F.countDistinct("player_id").alias("jogadores_unicos"),
        F.countDistinct("match_id").alias("partidas"),
        F.countDistinct("team_name").alias("times"),
    ).show(truncate=False)

print("\nPosições mais frequentes:")
lineups.groupBy("position_name").count() \
    .orderBy("count", ascending=False) \
    .show(15, truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. Amostra — escalação Brasil na final WC 2022

# COMMAND ----------

lineups.filter(
    (F.col("_competition_label") == "FIFA World Cup 2022") &
    (F.col("team_name") == "Brazil")
).select(
    "match_id","player_name","jersey_number",
    "position_name","country_name","n_cards"
).orderBy("jersey_number").show(30, truncate=False)
