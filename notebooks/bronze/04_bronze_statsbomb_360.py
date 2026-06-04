# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — StatsBomb 360 (Freeze Frames)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.statsbomb_360`
# MAGIC **Disponível em:** WC 2022 · Euro 2024 · Euro 2020 (competições com 360°)
# MAGIC **Executar depois de:** 02_bronze_statsbomb_events
# MAGIC
# MAGIC Os dados 360° contêm, para cada evento de uma partida,
# MAGIC a posição [x,y] de todos os jogadores visíveis em câmera
# MAGIC no momento do evento — incluindo se o jogador é o ator
# MAGIC do evento, companheiro ou adversário.
# MAGIC
# MAGIC Uso principal na Silver/Gold:
# MAGIC   - Distância do defensor mais próximo no momento do chute → feature de xG
# MAGIC   - Número de adversários entre o chutador e o gol
# MAGIC   - Intensidade de pressing contextual por zona

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

DBFS_BASE  = "/Volumes/lakehouse/wc_platform/files/raw/statsbomb"
CATALOG    = "lakehouse"
SCHEMA     = "bronze"
TABLE      = "statsbomb_360"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Só competições com dados 360
COMPETITIONS_360 = [
    {"slug": "wc_2022",    "competition_id": 43, "season_id": 106, "label": "FIFA World Cup 2022"},
    {"slug": "euro_2024",  "competition_id": 55, "season_id": 282, "label": "UEFA Euro 2024"},
    {"slug": "euro_2020",  "competition_id": 55, "season_id": 43,  "label": "UEFA Euro 2020"},
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

# MAGIC %md ## 2. Leitura dos freeze frames
# MAGIC
# MAGIC Schema do arquivo three-sixty/{match_id}.json:
# MAGIC ```
# MAGIC [
# MAGIC   {
# MAGIC     "id": "event_uuid",
# MAGIC     "match_id": 12345,
# MAGIC     "location": [x, y],
# MAGIC     "teammate": true/false,
# MAGIC     "actor": true/false,
# MAGIC     "keeper": true/false,
# MAGIC     "visible_area": [x1, y1, x2, y2, ...]
# MAGIC   },
# MAGIC   ...
# MAGIC ]
# MAGIC ```
# MAGIC Cada linha no JSON já representa **um jogador visível** em um evento.
# MAGIC Não há campos aninhados `player` ou `position` — apenas localização e flags
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType, BooleanType, DoubleType

spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

for comp in COMPETITIONS_360:
    path_360 = f"{DBFS_BASE}/{comp['slug']}/three-sixty"

    try:
        files = dbutils.fs.ls(path_360)
    except Exception:
        print(f"⚠️  {comp['label']}: dados 360 não encontrados em {path_360}")
        continue

    print(f"\n📂 {comp['label']}: {len(files)} arquivos 360")

    raw = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path_360)
    )

    # Nível 1: explodir eventos (cada linha = 1 evento com freeze_frame array)
    events_df = raw.select(
        F.col("id").alias("event_id"),
        F.col("match_id").cast(LongType()),
        F.col("freeze_frame"),
        F.col("visible_area"),
        F.lit(comp["competition_id"]).cast(IntegerType()).alias("_competition_id"),
        F.lit(comp["label"]).alias("_competition_label"),
        F.current_timestamp().alias("_ingested_at"),
    )

    # Nível 2: explodir freeze_frame — 1 linha por jogador visível por evento
    frames_df = events_df.select(
        "event_id",
        "match_id",
        "_competition_id",
        "_competition_label",
        "_ingested_at",
        F.explode(F.col("freeze_frame")).alias("frame"),
    ).select(
        "event_id",
        "match_id",
        "_competition_id",
        "_competition_label",
        "_ingested_at",
        # Posição do jogador no campo
        F.col("frame.location")[0].cast(DoubleType()).alias("loc_x"),
        F.col("frame.location")[1].cast(DoubleType()).alias("loc_y"),
        # Identidade
        F.col("frame.player.id").cast(IntegerType()).alias("player_id"),
        F.col("frame.player.name").alias("player_name"),
        F.col("frame.position.id").cast(IntegerType()).alias("position_id"),
        F.col("frame.position.name").alias("position_name"),
        # Relação com o ator do evento
        F.col("frame.teammate").cast(BooleanType()).alias("is_teammate"),
        # actor = o jogador que executou o evento
        F.col("frame.actor").cast(BooleanType()).alias("is_actor"),
        # keeper = goleiro
        F.col("frame.keeper").cast(BooleanType()).alias("is_keeper"),
    )

    n = frames_df.count()
    print(f"   Registros (jogador x evento): {n:,}")

    frames_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_competition_id") \
        .saveAsTable(FULL_TABLE)

    print(f"   ✅ {comp['label']} escrita")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType, BooleanType, DoubleType

spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

for comp in COMPETITIONS_360:
    path_360 = f"{DBFS_BASE}/{comp['slug']}/three-sixty"

    try:
        files = dbutils.fs.ls(path_360)
    except Exception:
        print(f"⚠️  {comp['label']}: dados 360 não encontrados em {path_360}")
        continue

    print(f"\n📂 {comp['label']}: {len(files)} arquivos 360")

    raw = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(path_360)
    )

    # O JSON já vem "explodido" — cada linha = 1 jogador visível em 1 evento
    # Não há array freeze_frame para explodir, nem campos player/position aninhados
    frames_df = raw.select(
        F.col("id").alias("event_id"),
        F.col("match_id").cast(LongType()),
        # Posição do jogador no campo (array de 2 elementos: [x, y])
        F.col("location")[0].cast(DoubleType()).alias("loc_x"),
        F.col("location")[1].cast(DoubleType()).alias("loc_y"),
        # Flags de relação com o evento (não há player_id/name/position neste formato)
        F.col("teammate").cast(BooleanType()).alias("is_teammate"),
        F.col("actor").cast(BooleanType()).alias("is_actor"),
        F.col("keeper").cast(BooleanType()).alias("is_keeper"),
        # Metadados
        F.lit(comp["competition_id"]).cast(IntegerType()).alias("_competition_id"),
        F.lit(comp["label"]).alias("_competition_label"),
        F.current_timestamp().alias("_ingested_at"),
    )

    n = frames_df.count()
    print(f"   Registros (jogador x evento): {n:,}")

    frames_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_competition_id") \
        .saveAsTable(FULL_TABLE)

    print(f"   ✅ {comp['label']} escrita")

# COMMAND ----------

# MAGIC %md ## 3. Validação

# COMMAND ----------

df_360 = spark.table(FULL_TABLE)
print(f"Total de registros 360: {df_360.count():,}")

print("\nRegistros por competição:")
df_360.groupBy("_competition_label") \
    .agg(
        F.count("*").alias("total_registros"),
        F.countDistinct("event_id").alias("eventos_com_360"),
        F.countDistinct("match_id").alias("partidas"),
    ).show(truncate=False)

print("\nMédia de jogadores visíveis por evento:")
df_360.groupBy("event_id") \
    .count() \
    .agg(F.avg("count").alias("avg_jogadores_visiveis")) \
    .show()

# COMMAND ----------

# MAGIC %md ## 4. Preview — contexto de um chute com 360°
# MAGIC
# MAGIC Cruzar com tabela de eventos para ver o freeze frame de um gol famoso.

# COMMAND ----------

# Pegar um shot_xg alto do WC 2022 e mostrar quem estava visível
shots_360 = spark.table(f"{CATALOG}.{SCHEMA}.statsbomb_events") \
    .filter(
        (F.col("type_name") == "Shot") &
        (F.col("_competition_id") == 43) &
        (F.col("shot_xg") > 0.5)
    ) \
    .select("event_id","match_id","minute","player_name","team_name","shot_xg","shot_outcome") \
    .limit(3)

shots_360.show(truncate=False)

# Para cada shot, mostrar o freeze frame (nota: não temos player_name/position_name no 360)
for row in shots_360.collect():
    print(f"\n🎯 Chute de {row['player_name']} ({row['team_name']}) — min {row['minute']} | xG={row['shot_xg']:.3f} | {row['shot_outcome']}")
    freeze = df_360.filter(F.col("event_id") == row["event_id"]) \
        .select("loc_x","loc_y","is_teammate","is_actor","is_keeper")
    print(f"   Jogadores visíveis: {freeze.count()}")
    freeze.show(25, truncate=False)
