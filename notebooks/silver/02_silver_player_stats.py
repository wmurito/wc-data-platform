# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Player Stats
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver
# MAGIC **Tabela:** `worldcup.silver.player_match_stats`
# MAGIC **Fonte:** `worldcup.silver.match_timeline` + `worldcup.bronze.statsbomb_lineups` + `worldcup.bronze.fbref_player_season`
# MAGIC **Executar depois de:** 01_silver_match_timeline + 03_bronze_statsbomb_lineups + 05_bronze_fbref_players
# MAGIC
# MAGIC Agrega os eventos da timeline por jogador x partida.
# MAGIC Resultado: 1 linha por jogador por partida com todas as métricas consolidadas.
# MAGIC
# MAGIC **Métricas calculadas:**
# MAGIC - Chutes: total, no alvo, gols, xG acumulado, xG/chute
# MAGIC - Passes: total, completos, taxa de conclusão, passes-chave, xA, crosses, passes progressivos
# MAGIC - Pressão: pressões aplicadas, % em zona alta (x > 80)
# MAGIC - Carries: progressive carries, distância total transportada
# MAGIC - Duelos: ganhos/perdidos, % vitória
# MAGIC - Enriquecimento: stats FBref da temporada 2024-25 do clube (xG/90, npxG)

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

CATALOG    = "lakehouse"
SCHEMA_S   = "silver"
SCHEMA_B   = "bronze"
TABLE      = "player_match_stats"
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

# MAGIC %md ## 1. Leitura das fontes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

timeline  = spark.table(f"{CATALOG}.{SCHEMA_S}.match_timeline")
lineups   = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_lineups")
fbref     = spark.table(f"{CATALOG}.{SCHEMA_B}.fbref_player_season") \
                 .filter(F.col("stat_category") == "shooting")

print(f"Timeline:  {timeline.count():,} eventos")
print(f"Lineups:   {lineups.count():,} jogadores x partida")
print(f"FBref:     {fbref.count():,} registros de shooting")

# COMMAND ----------

# MAGIC %md ## 2. Agregações por jogador x partida

# COMMAND ----------

# ── 2.1 Shots
shots_agg = timeline.filter(F.col("type_name") == "Shot") \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.count("*").alias("shots_total"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
        F.sum(F.when(F.col("shot_outcome").isin("Goal","Saved"), 1).otherwise(0)).alias("shots_on_target"),
        F.sum(F.col("shot_xg")).alias("xg_total"),
        F.avg(F.col("shot_xg")).alias("xg_per_shot"),
        F.sum(F.when(F.col("shot_first_time") == True, 1).otherwise(0)).alias("shots_first_time"),
        F.sum(F.when(F.col("shot_one_on_one") == True, 1).otherwise(0)).alias("shots_one_on_one"),
        F.sum(F.when(F.col("shot_body_part") == "Head", 1).otherwise(0)).alias("shots_head"),
        F.sum(F.when(F.col("shot_type") == "Penalty", 1).otherwise(0)).alias("penalties_taken"),
        F.sum(F.when(
            (F.col("shot_type") == "Penalty") & (F.col("shot_outcome") == "Goal"), 1
        ).otherwise(0)).alias("penalties_scored"),
        F.avg(F.col("distance_to_goal")).alias("avg_shot_distance"),
        F.avg(F.col("angle_to_goal")).alias("avg_shot_angle"),
    )

# ── 2.2 Passes
passes_agg = timeline.filter(F.col("type_name") == "Pass") \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.count("*").alias("passes_total"),
        F.sum(F.when(F.col("pass_outcome").isNull(), 1)
               .otherwise(0)).alias("passes_complete"),  # null outcome = complete no SB
        F.sum(F.col("pass_xa")).alias("xa_total"),
        F.sum(F.when(F.col("pass_shot_assist") == True, 1).otherwise(0)).alias("key_passes"),
        F.sum(F.when(F.col("pass_goal_assist") == True, 1).otherwise(0)).alias("assists"),
        F.sum(F.when(F.col("pass_cross") == True, 1).otherwise(0)).alias("crosses"),
        F.sum(F.when(F.col("pass_switch") == True, 1).otherwise(0)).alias("switches"),
        F.sum(F.when(F.col("pass_through_ball") == True, 1).otherwise(0)).alias("through_balls"),
        F.sum(F.when(F.col("pass_type") == "Corner", 1).otherwise(0)).alias("corners"),
        F.sum(F.when(F.col("pass_type") == "Free Kick", 1).otherwise(0)).alias("free_kicks"),
        # Passes progressivos: end_x aumentou >= 10m em direção ao gol
        F.sum(F.when(
            (F.col("pass_end_x") - F.col("loc_x") >= 10) &
            (F.col("pass_end_x") > 60), 1
        ).otherwise(0)).alias("progressive_passes"),
        F.avg(F.col("pass_length")).alias("avg_pass_length"),
    ) \
    .withColumn("pass_completion_pct",
        F.when(F.col("passes_total") > 0,
            F.round(F.col("passes_complete") / F.col("passes_total") * 100, 1)
        ).otherwise(F.lit(None))
    )

# ── 2.3 Pressões (pressing)
pressure_agg = timeline.filter(F.col("type_name") == "Pressure") \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.count("*").alias("pressures_total"),
        # Pressão em zona alta (terço final do campo adversário)
        F.sum(F.when(F.col("pitch_zone") == "attacking_third", 1).otherwise(0)).alias("pressures_high"),
        F.sum(F.when(F.col("counterpress") == True, 1).otherwise(0)).alias("counterpresses"),
    ) \
    .withColumn("pressure_high_pct",
        F.when(F.col("pressures_total") > 0,
            F.round(F.col("pressures_high") / F.col("pressures_total") * 100, 1)
        ).otherwise(F.lit(None))
    )

# ── 2.4 Carries (condução)
carries_agg = timeline.filter(F.col("type_name") == "Carry") \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.count("*").alias("carries_total"),
        # Progressive carry: ganhou >= 5m em direção ao gol e terminou no terço final
        F.sum(F.when(
            (F.col("carry_end_x") - F.col("loc_x") >= 5) &
            (F.col("carry_end_x") > 60), 1
        ).otherwise(0)).alias("progressive_carries"),
        # Distância total transportada
        F.sum(
            F.sqrt(
                F.pow(F.col("carry_end_x") - F.col("loc_x"), 2) +
                F.pow(F.col("carry_end_y") - F.col("loc_y"), 2)
            )
        ).alias("carry_distance_total"),
    )

# ── 2.5 Duelos
duels_agg = timeline.filter(F.col("type_name") == "Duel") \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.count("*").alias("duels_total"),
        F.sum(F.when(F.col("duel_outcome").isin("Won","Success"), 1).otherwise(0)).alias("duels_won"),
        F.sum(F.when(F.col("duel_type") == "Tackle", 1).otherwise(0)).alias("tackles"),
    ) \
    .withColumn("duel_win_pct",
        F.when(F.col("duels_total") > 0,
            F.round(F.col("duels_won") / F.col("duels_total") * 100, 1)
        ).otherwise(F.lit(None))
    )

# ── 2.6 Interceptações e Faltas
misc_agg = timeline.filter(F.col("type_name").isin("Interception","Foul Committed","Foul Won")) \
    .groupBy("match_id", "player_id", "player_name", "team_id", "team_name",
             "_competition_id", "_competition_label", "_season_id") \
    .agg(
        F.sum(F.when(F.col("type_name") == "Interception", 1).otherwise(0)).alias("interceptions"),
        F.sum(F.when(F.col("type_name") == "Foul Committed", 1).otherwise(0)).alias("fouls_committed"),
        F.sum(F.when(F.col("type_name") == "Foul Won", 1).otherwise(0)).alias("fouls_won"),
        F.sum(F.when(F.col("foul_card").isin("Yellow Card"), 1).otherwise(0)).alias("yellow_cards"),
        F.sum(F.when(F.col("foul_card").isin("Red Card","Second Yellow"), 1).otherwise(0)).alias("red_cards"),
    )

# COMMAND ----------

# MAGIC %md ## 3. Consolidar todas as agregações

# COMMAND ----------

# DBTITLE 1,Cell 9
# Base: todos jogadores que apareceram na timeline (com ou sem shots)
player_base = timeline.filter(
    F.col("player_id").isNotNull()
).select(
    "match_id","player_id","player_name","team_id","team_name",
    "_competition_id","_competition_label","_season_id","match_date",
    "competition_name","season_name","stage_name",
    "home_team_name","away_team_name","is_home_team",
    "match_home_score_ft","match_away_score_ft"
).distinct()

# Minutos jogados — estimado pelo último evento do jogador na partida
minutes_played = timeline.filter(F.col("player_id").isNotNull()) \
    .groupBy("match_id","player_id") \
    .agg(F.max("minute").alias("last_minute_seen")) \
    .withColumnRenamed("player_id","_pid")

player_stats = player_base \
    .join(shots_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(passes_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(pressure_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(carries_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(duels_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(misc_agg.drop("player_name","team_id","team_name","_competition_label","_season_id"),
          on=["match_id","player_id","_competition_id"], how="left") \
    .join(minutes_played, on=[player_base["match_id"]==minutes_played["match_id"],
                               player_base["player_id"]==minutes_played["_pid"]], how="left") \
    .drop("_pid", minutes_played["match_id"]) \
    .fillna(0, subset=[
        "shots_total","goals","shots_on_target","xg_total",
        "passes_total","passes_complete","xa_total","key_passes","assists",
        "pressures_total","pressures_high","counterpresses",
        "carries_total","progressive_carries",
        "duels_total","duels_won","tackles",
        "interceptions","fouls_committed","fouls_won","yellow_cards","red_cards",
    ])

# COMMAND ----------

# MAGIC %md ## 4. Enriquecimento com FBref (stats de clube 2024-25)

# COMMAND ----------

# Normalizar nome do jogador para join aproximado
# FBref usa nomes sem acento às vezes — join por similaridade de nome
fbref_slim = fbref.select(
    F.col("player_name").alias("fbref_name"),
    F.col("club"),
    F.col("league"),
    F.col("goals").alias("fbref_goals_club"),
    F.col("xg").alias("fbref_xg_club"),
    F.col("npxg").alias("fbref_npxg_club"),
    F.col("xg_per_shot").alias("fbref_xg_per_shot"),
    F.col("shots").alias("fbref_shots_club"),
).dropDuplicates(["fbref_name"])

# Join por player_name (best effort — não vai casar 100%)
player_stats_enriched = player_stats.join(
    fbref_slim,
    player_stats["player_name"] == fbref_slim["fbref_name"],
    how="left"
).drop("fbref_name")

print(f"Player stats consolidadas: {player_stats_enriched.count():,} linhas")

# COMMAND ----------

# MAGIC %md ## 5. Adicionar posição do jogador (via lineups)

# COMMAND ----------

lineups_slim = lineups.select(
    "match_id",
    "player_id",
    "position_name",
    "jersey_number",
    "country_name",
).dropDuplicates(["match_id","player_id"])

player_stats_final = player_stats_enriched.join(
    lineups_slim, on=["match_id","player_id"], how="left"
).withColumn("_processed_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md ## 6. MERGE upsert

# COMMAND ----------

if spark.catalog.tableExists(FULL_TABLE):
    dt = DeltaTable.forName(spark, FULL_TABLE)
    dt.alias("t").merge(
        player_stats_final.alias("s"),
        "t.player_id = s.player_id AND t.match_id = s.match_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"✅ MERGE executado: {FULL_TABLE}")
else:
    player_stats_final.write \
        .format("delta").mode("overwrite") \
        .option("overwriteSchema","true") \
        .partitionBy("_competition_id") \
        .saveAsTable(FULL_TABLE)
    print(f"✅ Tabela criada: {FULL_TABLE}")

# COMMAND ----------

# MAGIC %md ## 7. Validação — Top artilheiros e criadores

# COMMAND ----------

ps = spark.table(FULL_TABLE)
print(f"Total registros: {ps.count():,}")

print("\n🏆 Top artilheiros (gols + xG) por competição:")
ps.groupBy("_competition_label","player_name","team_name") \
    .agg(
        F.sum("goals").alias("gols"),
        F.sum("xg_total").alias("xg_acum"),
        F.sum("assists").alias("assists"),
        F.sum("shots_total").alias("chutes"),
    ) \
    .withColumn("gols_minus_xg", F.round(F.col("gols") - F.col("xg_acum"), 2)) \
    .filter(F.col("gols") >= 1) \
    .orderBy(F.col("gols").desc(), F.col("xg_acum").desc()) \
    .show(20, truncate=False)

print("\n⚡ Top pressers (pressing alto):")
ps.groupBy("player_name","team_name","_competition_label") \
    .agg(F.sum("pressures_high").alias("pressoes_zona_alta")) \
    .filter(F.col("pressoes_zona_alta") >= 10) \
    .orderBy(F.col("pressoes_zona_alta").desc()) \
    .show(15, truncate=False)
