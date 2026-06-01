# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Shot Map (com contexto 360°)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver
# MAGIC **Tabela:** `worldcup.silver.shot_map`
# MAGIC **Fonte:** `worldcup.silver.match_timeline` + `worldcup.bronze.statsbomb_360`
# MAGIC **Executar depois de:** 01_silver_match_timeline + 04_bronze_statsbomb_360
# MAGIC
# MAGIC Tabela especializada em chutes — combina os dados do chute com
# MAGIC o contexto tático do freeze frame (dados 360°).
# MAGIC
# MAGIC **Features calculadas a partir do freeze frame:**
# MAGIC - `defenders_in_cone`: defensores no cone de chute (ângulo de 45° ao gol)
# MAGIC - `distance_nearest_defender`: distância do defensor mais próximo
# MAGIC - `gk_distance_to_goal`: distância do goleiro à linha do gol (estava saindo?)
# MAGIC - `open_angle`: ângulo efetivo ao gol após descontar defensores
# MAGIC
# MAGIC Esta é a tabela que alimenta o modelo de xG da Fase ML.

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_B   = "bronze"
TABLE      = "shot_map"
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
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

timeline = spark.table(f"{CATALOG}.{SCHEMA_S}.match_timeline")
frames   = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_360")

# COMMAND ----------
# MAGIC %md ## 1. Filtrar só chutes da timeline

# COMMAND ----------
shots = timeline.filter(
    (F.col("type_name") == "Shot") &
    F.col("shot_xg").isNotNull()
).select(
    "event_id","match_id","minute","second","period",
    "player_id","player_name","team_id","team_name","position_name",
    "loc_x","loc_y",
    "shot_xg","shot_outcome","shot_technique","shot_body_part","shot_type",
    "shot_first_time","shot_one_on_one",
    "shot_end_x","shot_end_y",
    "distance_to_goal","angle_to_goal","pitch_zone",
    "under_pressure",
    "score_home_at_event","score_away_at_event","score_diff_at_event",
    "is_home_team",
    "match_date","competition_name","stage_name",
    "_competition_id","_competition_label","_season_id",
)

print(f"Chutes totais: {shots.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Agregar contexto 360° por chute
# MAGIC
# MAGIC Para cada chute, calcular métricas a partir do freeze frame.
# MAGIC Só disponível para WC 2022, Euro 2024 e Euro 2020.

# COMMAND ----------
import math

# Constantes do campo StatsBomb
GOAL_X   = 120.0
GOAL_Y   = 40.0
GOAL_LEFT  = 36.0   # poste esquerdo
GOAL_RIGHT = 44.0   # poste direito

# ── Distância do defensor mais próximo ao chutador
nearest_def = frames.filter(
    (F.col("is_teammate") == False) & (F.col("is_actor") == False)
).select(
    "event_id","match_id",
    F.col("loc_x").alias("def_x"),
    F.col("loc_y").alias("def_y"),
    "is_keeper",
)

# Join com shots para calcular distância euclidiana
shots_with_def = shots.join(
    nearest_def, on=["event_id","match_id"], how="left"
)

# Distância do defensor ao chutador
shots_def_dist = shots_with_def \
    .withColumn("def_dist",
        F.sqrt(
            F.pow(F.col("loc_x") - F.col("def_x"), 2) +
            F.pow(F.col("loc_y") - F.col("def_y"), 2)
        )
    )

# Para cada chute: distância ao defensor mais próximo (non-keeper)
min_def_dist = shots_def_dist.filter(F.col("is_keeper") == False) \
    .groupBy("event_id","match_id") \
    .agg(F.min("def_dist").alias("distance_nearest_defender"))

# Distância do goleiro à linha do gol
gk_dist = shots_def_dist.filter(F.col("is_keeper") == True) \
    .groupBy("event_id","match_id") \
    .agg(
        F.min("def_dist").alias("gk_distance_to_shooter"),
        F.avg(F.abs(F.lit(GOAL_X) - F.col("def_x"))).alias("gk_distance_to_goalline"),
    )

# Defensores no cone de chute (triângulo: chutador → postes do gol)
# Um defensor está no cone se sua posição está dentro do triângulo
# Simplificação: x entre chutador e gol, y entre postes expandidos
cone_defenders = shots_with_def.filter(
    (F.col("is_keeper") == False) &
    (F.col("def_x") > F.col("loc_x")) &  # à frente do chutador
    (F.col("def_x") <= GOAL_X) &
    (F.col("def_y") >= GOAL_LEFT - 2) &   # dentro da largura do gol + 2m
    (F.col("def_y") <= GOAL_RIGHT + 2)
).groupBy("event_id","match_id") \
 .agg(F.count("*").alias("defenders_in_cone"))

# Companheiros de time visíveis
teammates_visible = frames.filter(
    (F.col("is_teammate") == True) & (F.col("is_actor") == False)
).groupBy("event_id","match_id") \
 .agg(F.count("*").alias("teammates_visible"))

# Total jogadores visíveis
total_visible = frames.groupBy("event_id","match_id") \
    .agg(F.count("*").alias("total_players_visible"))

# COMMAND ----------
# MAGIC %md ## 3. Consolidar shot map

# COMMAND ----------
shot_map = shots \
    .join(min_def_dist,    on=["event_id","match_id"], how="left") \
    .join(gk_dist,         on=["event_id","match_id"], how="left") \
    .join(cone_defenders,  on=["event_id","match_id"], how="left") \
    .join(teammates_visible, on=["event_id","match_id"], how="left") \
    .join(total_visible,   on=["event_id","match_id"], how="left") \
    .fillna(0, subset=["defenders_in_cone","teammates_visible","total_players_visible"]) \
    .withColumn("has_360_data",
        F.col("distance_nearest_defender").isNotNull()
    ) \
    .withColumn("is_goal",
        F.when(F.col("shot_outcome") == "Goal", True).otherwise(False)
    ) \
    .withColumn("_processed_at", F.current_timestamp())

print(f"Shot map total: {shot_map.count():,} chutes")
print(f"  Com dados 360°: {shot_map.filter(F.col('has_360_data')).count():,}")
print(f"  Sem dados 360°: {shot_map.filter(~F.col('has_360_data')).count():,}")

# COMMAND ----------
# MAGIC %md ## 4. MERGE upsert

# COMMAND ----------
if spark.catalog.tableExists(FULL_TABLE):
    dt = DeltaTable.forName(spark, FULL_TABLE)
    dt.alias("t").merge(
        shot_map.alias("s"),
        "t.event_id = s.event_id AND t.match_id = s.match_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✅ MERGE: {FULL_TABLE}")
else:
    shot_map.write.format("delta").mode("overwrite") \
        .option("overwriteSchema","true") \
        .partitionBy("_competition_id") \
        .saveAsTable(FULL_TABLE)
    print(f"✅ Criada: {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Análises exploratórias

# COMMAND ----------
sm = spark.table(FULL_TABLE)
print(f"Total chutes: {sm.count():,}")

print("\n📐 Distribuição de xG por zona do campo:")
sm.groupBy("pitch_zone") \
    .agg(
        F.count("*").alias("chutes"),
        F.sum("is_goal".cast(IntegerType())).alias("gols"),
        F.avg("shot_xg").alias("xg_medio"),
        F.sum("shot_xg").alias("xg_total"),
    ) \
    .withColumn("conversion_pct",
        F.round(F.col("gols") / F.col("chutes") * 100, 1)
    ) \
    .orderBy("xg_medio", ascending=False) \
    .show(truncate=False)

print("\n🎯 xG real vs esperado por técnica de chute:")
sm.groupBy("shot_technique") \
    .agg(
        F.count("*").alias("chutes"),
        F.sum(F.col("is_goal").cast(IntegerType())).alias("gols"),
        F.avg("shot_xg").alias("xg_medio"),
        F.avg("distance_to_goal").alias("dist_media"),
    ) \
    .filter(F.col("chutes") >= 20) \
    .orderBy("xg_medio", ascending=False) \
    .show(truncate=False)

print("\n🏟️ Gols acima do xG (overperformers na Copa 2022):")
sm.filter(F.col("_competition_label") == "FIFA World Cup 2022") \
    .groupBy("player_name","team_name") \
    .agg(
        F.sum(F.col("is_goal").cast(IntegerType())).alias("gols"),
        F.sum("shot_xg").alias("xg_esperado"),
        F.count("*").alias("chutes"),
    ) \
    .withColumn("gols_acima_xg",
        F.round(F.col("gols") - F.col("xg_esperado"), 2)
    ) \
    .filter(F.col("gols") >= 2) \
    .orderBy("gols_acima_xg", ascending=False) \
    .show(15, truncate=False)
