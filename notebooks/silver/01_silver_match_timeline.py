# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Match Timeline
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver
# MAGIC **Tabela:** `worldcup.silver.match_timeline`
# MAGIC **Fonte:** `worldcup.bronze.statsbomb_events` + `worldcup.bronze.statsbomb_matches`
# MAGIC **Executar depois de:** 02_bronze_statsbomb_events + 01_bronze_statsbomb_matches
# MAGIC
# MAGIC Transforma os eventos brutos da Bronze em uma timeline limpa e enriquecida.
# MAGIC Foco: deduplica, normaliza tipos de evento, adiciona contexto da partida
# MAGIC (fase, times, placar acumulado) e calcula métricas derivadas por evento.
# MAGIC
# MAGIC **O que esta tabela entrega:**
# MAGIC - 1 linha por evento significativo (sem Ball Receipt, Camera On/Off, etc.)
# MAGIC - Placar acumulado no momento de cada evento
# MAGIC - Distância ao gol calculada para passes e chutes
# MAGIC - Flag de eventos em situação de pressing (counterpress)
# MAGIC - Chave `match_id + index` para MERGE idempotente

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

CATALOG    = "lakehouse"
SCHEMA_B   = "bronze"
SCHEMA_S   = "silver"
TABLE      = "match_timeline"
FULL_TABLE = f"{CATALOG}.{SCHEMA_S}.{TABLE}"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_S}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_B   = "wc_bronze"
    SCHEMA_S   = "wc_silver"
    FULL_TABLE = f"{CATALOG}.{SCHEMA_S}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_S}")

# COMMAND ----------

# MAGIC %md ## 1. Leitura da Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

events = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_events")
matches = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_matches")

print(f"Eventos Bronze: {events.count():,}")
print(f"Partidas Bronze: {matches.count():,}")

# COMMAND ----------

# MAGIC %md ## 2. Filtrar tipos de evento relevantes
# MAGIC
# MAGIC Remover eventos de sistema/câmera que não têm valor analítico.
# MAGIC Preservar todos os eventos com impacto tático/estatístico.

# COMMAND ----------

# Tipos a EXCLUIR (ruído operacional do StatsBomb)
EXCLUDE_TYPES = {
    "Ball Receipt*",        # confirmação de recebimento (redundante com Pass)
    "Camera On",
    "Camera Off",
    "50/50",                # disputas aéreas — mantidas em duel
    "Injury Stoppage",
    "Referee Ball-Drop",
    "Error",
}

# Tipos a MANTER com enriquecimento específico
INCLUDE_TYPES = {
    "Pass", "Shot", "Carry", "Pressure", "Duel",
    "Interception", "Clearance", "Block", "Goal Keeper",
    "Foul Committed", "Foul Won", "Dribble", "Dispossessed",
    "Miscontrol", "Ball Recovery", "Substitution",
    "Tactical Shift", "Starting XI",
    "Offside", "Shield",
}

events_filtered = events.filter(
    F.col("type_name").isin(list(INCLUDE_TYPES))
)
print(f"Eventos após filtro: {events_filtered.count():,}")
print(f"Eventos removidos: {events.count() - events_filtered.count():,}")

# COMMAND ----------

# MAGIC %md ## 3. Enriquecimento com dados da partida

# COMMAND ----------

# DBTITLE 1,Cell 9
# Selecionar apenas o que precisamos de matches
matches_slim = matches.select(
    "match_id",
    "match_date",
    "competition_name",
    "season_name",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_score",           # placar final (para referência)
    "away_score",
    "stage_name",
    "stadium_name",
    "_competition_id",
    "_season_id",
    "_competition_label",
)

# Drop ambiguous columns from events before join
# (we want the match-level versions from matches_slim)
events_enriched = events_filtered.drop(
    "_competition_id", "_season_id", "_competition_label"
).join(
    matches_slim, on="match_id", how="left"
)

# COMMAND ----------

# MAGIC %md ## 4. Features derivadas por evento

# COMMAND ----------

# DBTITLE 1,Cell 11
# Coordenadas do gol adversário (campo StatsBomb: 120x80)
# Gol do adversário está em x=120, y=40
GOAL_X = 120.0
GOAL_Y = 40.0

timeline = events_enriched.select(
    # ── Chave primária
    F.col("event_id"),
    F.col("match_id"),
    F.col("index").alias("event_index"),

    # ── Contexto temporal
    F.col("period"),
    F.col("timestamp"),
    F.col("minute"),
    F.col("second"),
    F.col("duration"),

    # ── Tipo
    F.col("type_name"),
    F.col("play_pattern_name"),

    # ── Time e jogador
    F.col("team_id"),
    F.col("team_name"),
    F.col("player_id"),
    F.col("player_name"),
    F.col("position_name"),

    # ── Localização
    F.col("location")[0].cast(DoubleType()).alias("loc_x"),
    F.col("location")[1].cast(DoubleType()).alias("loc_y"),

    # ── Distância ao gol (Euclidiana no campo 120x80)
    F.when(
        F.col("location").isNotNull(),
        F.sqrt(
            F.pow(F.lit(GOAL_X) - F.col("location")[0].cast(DoubleType()), 2) +
            F.pow(F.lit(GOAL_Y) - F.col("location")[1].cast(DoubleType()), 2)
        )
    ).alias("distance_to_goal"),

    # ── Ângulo ao gol (em graus)
    F.when(
        F.col("location").isNotNull(),
        F.degrees(
            F.atan2(
                F.abs(F.lit(GOAL_Y) - F.col("location")[1].cast(DoubleType())),
                F.abs(F.lit(GOAL_X) - F.col("location")[0].cast(DoubleType()))
            )
        )
    ).alias("angle_to_goal"),

    # ── Zona do campo (terços)
    F.when(F.col("location")[0] < 40,  F.lit("defensive_third"))
     .when(F.col("location")[0] < 80,  F.lit("middle_third"))
     .when(F.col("location")[0] >= 80, F.lit("attacking_third"))
     .otherwise(F.lit(None)).alias("pitch_zone"),

    # ── Flags contextuais
    F.col("under_pressure"),
    F.col("counterpress"),
    F.col("out"),

    # ── Shot stats (só populados para type_name == 'Shot')
    F.col("shot_xg"),
    F.col("shot_outcome"),
    F.col("shot_technique"),
    F.col("shot_body_part"),
    F.col("shot_type"),
    F.col("shot_first_time"),
    F.col("shot_one_on_one"),
    F.col("shot_end_location")[0].cast(DoubleType()).alias("shot_end_x"),
    F.col("shot_end_location")[1].cast(DoubleType()).alias("shot_end_y"),

    # ── Pass stats
    F.col("pass_length"),
    F.col("pass_angle"),
    F.col("pass_height"),
    F.col("pass_outcome"),
    F.col("pass_type"),
    F.col("pass_cross"),
    F.col("pass_switch"),
    F.col("pass_through_ball"),
    F.col("pass_xa"),
    F.col("pass_goal_assist"),
    F.col("pass_shot_assist"),
    F.col("pass_recipient_id"),
    F.col("pass_recipient_name"),
    F.col("pass_end_location")[0].cast(DoubleType()).alias("pass_end_x"),
    F.col("pass_end_location")[1].cast(DoubleType()).alias("pass_end_y"),

    # ── Carry
    F.col("carry_end_location")[0].cast(DoubleType()).alias("carry_end_x"),
    F.col("carry_end_location")[1].cast(DoubleType()).alias("carry_end_y"),

    # ── Duel
    F.col("duel_type"),
    F.col("duel_outcome"),

    # ── Goalkeeper
    F.col("goalkeeper_type"),
    F.col("goalkeeper_outcome"),

    # ── Foul / cartão
    F.col("foul_card"),
    F.col("foul_penalty"),

    # ── Interception / Clearance
    F.col("interception_outcome"),
    F.col("clearance_body_part"),

    # ── Contexto da partida (do join com matches)
    F.col("match_date"),
    F.col("competition_name"),
    F.col("season_name"),
    F.col("stage_name"),
    F.col("stadium_name"),
    F.col("home_team_name"),
    F.col("away_team_name"),
    F.col("home_score").alias("match_home_score_ft"),
    F.col("away_score").alias("match_away_score_ft"),

    # ── Identifica se o time do evento é o mandante
    F.when(F.col("team_id") == F.col("home_team_id"), F.lit(True))
     .otherwise(F.lit(False)).alias("is_home_team"),

    # ── Meta
    F.col("_competition_id"),
    F.col("_season_id"),
    F.col("_competition_label"),
    F.current_timestamp().alias("_processed_at"),
)

# COMMAND ----------

# MAGIC %md ## 5. Placar acumulado por minuto (window function)
# MAGIC
# MAGIC Calcula o placar no momento de cada evento — não só o placar final.
# MAGIC Fundamental para features de ML (estado do jogo quando o chute ocorreu).

# COMMAND ----------

from pyspark.sql.window import Window

# Identificar gols
goals = timeline.filter(
    (F.col("type_name") == "Shot") &
    (F.col("shot_outcome") == "Goal")
).select(
    "match_id", "minute", "second", "event_index",
    "team_id", "home_team_name", "away_team_name",
    F.col("team_name").alias("scoring_team"),
    # 1 se o time que marcou é o home
    F.when(
        F.col("team_name") == F.col("home_team_name"), F.lit(1)
    ).otherwise(F.lit(0)).alias("home_goal"),
    F.when(
        F.col("team_name") != F.col("home_team_name"), F.lit(1)
    ).otherwise(F.lit(0)).alias("away_goal"),
)

# Window: acumula gols por partida até o evento atual
w_home = Window.partitionBy("match_id").orderBy("event_index").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
w_away = Window.partitionBy("match_id").orderBy("event_index").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

# Join goals de volta nos eventos para calcular placar acumulado
goals_cumsum = goals.withColumn(
    "home_goals_cumsum", F.sum("home_goal").over(w_home)
).withColumn(
    "away_goals_cumsum", F.sum("away_goal").over(w_away)
).select("match_id", "event_index", "home_goals_cumsum", "away_goals_cumsum")

# Join no timeline principal
timeline_with_score = timeline.join(
    goals_cumsum, on=["match_id", "event_index"], how="left"
).withColumn(
    "score_home_at_event",
    F.coalesce(F.col("home_goals_cumsum"), F.lit(0)).cast(IntegerType())
).withColumn(
    "score_away_at_event",
    F.coalesce(F.col("away_goals_cumsum"), F.lit(0)).cast(IntegerType())
).withColumn(
    "score_diff_at_event",
    F.col("score_home_at_event") - F.col("score_away_at_event")
).drop("home_goals_cumsum", "away_goals_cumsum")

print(f"Timeline com placar acumulado: {timeline_with_score.count():,} eventos")

# COMMAND ----------

# MAGIC %md ## 6. MERGE upsert na Silver (idempotente)

# COMMAND ----------

# DBTITLE 1,Cell 15
if spark.catalog.tableExists(FULL_TABLE):

    dt = DeltaTable.forName(spark, FULL_TABLE)
    dt.alias("target").merge(
        timeline_with_score.alias("source"),
        "target.event_id = source.event_id AND target.match_id = source.match_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"✅ MERGE executado em {FULL_TABLE}")

else:
    timeline_with_score.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("_competition_id", "period") \
        .saveAsTable(FULL_TABLE)
    print(f"✅ Tabela criada: {FULL_TABLE}")

# COMMAND ----------

# MAGIC %md ## 7. Validação

# COMMAND ----------

silver = spark.table(FULL_TABLE)
total  = silver.count()
print(f"Total eventos Silver: {total:,}")

print("\nEventos por tipo:")
silver.groupBy("type_name").count() \
    .orderBy("count", ascending=False).show(15, truncate=False)

print("\nChutes com maior xG (Top 10):")
silver.filter(
    (F.col("type_name") == "Shot") & F.col("shot_xg").isNotNull()
).select(
    "_competition_label","match_date","stage_name","minute",
    "team_name","player_name",
    "shot_xg","shot_outcome","shot_technique","distance_to_goal","angle_to_goal",
    "score_home_at_event","score_away_at_event"
).orderBy(F.col("shot_xg").desc()).show(10, truncate=False)

print("\nGols marcados por competição:")
silver.filter(
    (F.col("type_name") == "Shot") & (F.col("shot_outcome") == "Goal")
).groupBy("_competition_label") \
    .agg(
        F.count("*").alias("gols"),
        F.avg("shot_xg").alias("avg_xg_por_gol"),
        F.sum("shot_xg").alias("total_xg"),
    ).show(truncate=False)
