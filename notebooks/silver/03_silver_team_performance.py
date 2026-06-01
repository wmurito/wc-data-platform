# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Team Performance
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver
# MAGIC **Tabela:** `worldcup.silver.team_performance`
# MAGIC **Fonte:** `worldcup.silver.match_timeline` + `worldcup.bronze.statsbomb_matches`
# MAGIC **Executar depois de:** 01_silver_match_timeline
# MAGIC
# MAGIC Agrega os eventos por time x partida.
# MAGIC 1 linha = 1 time em 1 partida.
# MAGIC
# MAGIC **Métricas calculadas:**
# MAGIC - Posse de bola (% de eventos de posse)
# MAGIC - xG, xGA (expected goals against)
# MAGIC - PPDA — Passes Allowed Per Defensive Action (intensidade de pressing)
# MAGIC - Chutes totais, no alvo, bloqueados
# MAGIC - Passes progressivos, carries progressivos
# MAGIC - Profundidade de linha defensiva (avg x do clearance)
# MAGIC - Compactação (std deviation das posições y dos jogadores)

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_B   = "bronze"
TABLE      = "team_performance"
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
from delta.tables import DeltaTable

timeline = spark.table(f"{CATALOG}.{SCHEMA_S}.match_timeline")
matches  = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_matches")

# COMMAND ----------
# MAGIC %md ## 2. Posse de bola
# MAGIC
# MAGIC StatsBomb define posse como sequências de ações controladas.
# MAGIC Calculamos % de eventos de posse pertencentes a cada time.

# COMMAND ----------
# Total de eventos de posse por partida
total_possession_events = timeline.filter(
    F.col("type_name").isin(["Pass","Carry","Dribble","Shot","Ball Recovery"])
).groupBy("match_id") \
 .agg(F.count("*").alias("total_poss_events"))

# Por time
team_possession = timeline.filter(
    F.col("type_name").isin(["Pass","Carry","Dribble","Shot","Ball Recovery"])
).groupBy("match_id","team_id","team_name") \
 .agg(F.count("*").alias("team_poss_events")) \
 .join(total_possession_events, on="match_id") \
 .withColumn("possession_pct",
     F.round(F.col("team_poss_events") / F.col("total_poss_events") * 100, 1)
 )

# COMMAND ----------
# MAGIC %md ## 3. Chutes e xG

# COMMAND ----------
shots_team = timeline.filter(F.col("type_name") == "Shot") \
    .groupBy("match_id","team_id","team_name",
             "_competition_id","_competition_label","_season_id") \
    .agg(
        F.count("*").alias("shots_total"),
        F.sum(F.when(F.col("shot_outcome").isin("Goal","Saved"), 1).otherwise(0)).alias("shots_on_target"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals_scored"),
        F.sum(F.col("shot_xg")).alias("xg"),
        F.avg(F.col("shot_xg")).alias("xg_per_shot"),
        F.avg(F.col("distance_to_goal")).alias("avg_shot_distance"),
        F.sum(F.when(F.col("pitch_zone") == "attacking_third", 1).otherwise(0)).alias("shots_from_att_third"),
        F.sum(F.when(F.col("shot_type") == "Penalty", 1).otherwise(0)).alias("penalties"),
        F.sum(F.when(F.col("shot_body_part") == "Head", 1).otherwise(0)).alias("headers"),
    )

# COMMAND ----------
# MAGIC %md ## 4. xGA — Expected Goals Against
# MAGIC
# MAGIC Para cada partida, o xGA de um time = xG do adversário.

# COMMAND ----------
# xG do adversário por partida
xg_against = shots_team.select(
    "match_id",
    F.col("team_id").alias("_opp_team_id"),
    F.col("xg").alias("xga"),
    F.col("goals_scored").alias("goals_conceded"),
)

# Pegar o time adversário da tabela matches
matches_teams = matches.select(
    "match_id",
    "home_team_id","away_team_id"
)

# Para cada time, o xGA vem do xG do time adversário naquela partida
shots_team_with_xga = shots_team.join(matches_teams, on="match_id") \
    .withColumn("opponent_id",
        F.when(F.col("team_id") == F.col("home_team_id"), F.col("away_team_id"))
         .otherwise(F.col("home_team_id"))
    ) \
    .join(
        xg_against.withColumnRenamed("_opp_team_id","opponent_id"),
        on=["match_id","opponent_id"], how="left"
    ) \
    .drop("home_team_id","away_team_id","opponent_id")

# COMMAND ----------
# MAGIC %md ## 5. PPDA — Passes Allowed Per Defensive Action
# MAGIC
# MAGIC PPDA = passes do adversário / ações defensivas do time no terço do adversário
# MAGIC Quanto menor o PPDA, maior o pressing intensity.
# MAGIC PPDA < 8 = pressing intenso (estilo Liverpool/Man City)
# MAGIC PPDA > 14 = bloco baixo (estilo tradicional)

# COMMAND ----------
# Passes do adversário (ações que o time permitiu)
opp_passes = timeline.filter(F.col("type_name") == "Pass") \
    .join(matches_teams, on="match_id") \
    .withColumn("defending_team_id",
        F.when(F.col("team_id") == F.col("home_team_id"), F.col("away_team_id"))
         .otherwise(F.col("home_team_id"))
    ) \
    .groupBy("match_id", F.col("defending_team_id").alias("team_id")) \
    .agg(F.count("*").alias("opp_passes_allowed"))

# Ações defensivas no meio e terço ofensivo do adversário
# (pressing, tackles, interceptions no campo adversário x > 40)
def_actions = timeline.filter(
    F.col("type_name").isin(["Pressure","Interception","Tackle","Block"]) &
    (F.col("loc_x") > 40)   # só conta pressing no campo do adversário
).groupBy("match_id","team_id") \
 .agg(F.count("*").alias("defensive_actions_high"))

ppda_df = opp_passes.join(def_actions, on=["match_id","team_id"], how="left") \
    .withColumn("ppda",
        F.when(F.col("defensive_actions_high") > 0,
            F.round(F.col("opp_passes_allowed") / F.col("defensive_actions_high"), 2)
        ).otherwise(F.lit(None))
    )

# COMMAND ----------
# MAGIC %md ## 6. Passes e carries progressivos

# COMMAND ----------
progression = timeline.filter(
    F.col("type_name").isin(["Pass","Carry"])
).groupBy("match_id","team_id","team_name",
           "_competition_id","_competition_label","_season_id") \
 .agg(
     F.count("*").alias("actions_total"),
     F.sum(F.when(
         (F.col("type_name") == "Pass") &
         (F.col("pass_end_x") - F.col("loc_x") >= 10) &
         (F.col("pass_end_x") > 60), 1
     ).otherwise(0)).alias("progressive_passes"),
     F.sum(F.when(
         (F.col("type_name") == "Carry") &
         (F.col("carry_end_x") - F.col("loc_x") >= 5) &
         (F.col("carry_end_x") > 60), 1
     ).otherwise(0)).alias("progressive_carries"),
     F.sum(F.when(F.col("pass_cross") == True, 1).otherwise(0)).alias("crosses"),
     F.avg(F.col("pass_length")).alias("avg_pass_length"),
 )

# COMMAND ----------
# MAGIC %md ## 7. Profundidade defensiva
# MAGIC
# MAGIC Média da posição x dos clearances — quanto mais baixo, mais recuado o time.

# COMMAND ----------
defensive_depth = timeline.filter(F.col("type_name") == "Clearance") \
    .groupBy("match_id","team_id") \
    .agg(
        F.avg("loc_x").alias("avg_defensive_line_x"),
        F.count("*").alias("clearances"),
    )

# COMMAND ----------
# MAGIC %md ## 8. Consolidação final

# COMMAND ----------
# Contexto da partida
match_ctx = matches.select(
    "match_id","match_date","competition_name","season_name","stage_name",
    "home_team_id","home_team_name","away_team_id","away_team_name",
    "_competition_id","_season_id","_competition_label",
)

team_perf = shots_team_with_xga \
    .join(team_possession.drop("total_poss_events","team_poss_events"),
          on=["match_id","team_id","team_name"], how="left") \
    .join(ppda_df.drop("opp_passes_allowed","defensive_actions_high"),
          on=["match_id","team_id"], how="left") \
    .join(progression.drop("team_name","_competition_id","_competition_label","_season_id","actions_total"),
          on=["match_id","team_id"], how="left") \
    .join(defensive_depth, on=["match_id","team_id"], how="left") \
    .join(match_ctx, on=["match_id","_competition_id","_competition_label","_season_id"], how="left") \
    .withColumn("is_home",
        F.col("team_id") == F.col("home_team_id")
    ) \
    .withColumn("result",
        F.when(
            (F.col("is_home") == True) &
            (F.col("goals_scored") > F.col("goals_conceded")), F.lit("W")
        ).when(
            (F.col("is_home") == False) &
            (F.col("goals_scored") > F.col("goals_conceded")), F.lit("W")
        ).when(F.col("goals_scored") == F.col("goals_conceded"), F.lit("D"))
         .otherwise(F.lit("L"))
    ) \
    .withColumn("_processed_at", F.current_timestamp())

print(f"Team performance: {team_perf.count():,} registros (time x partida)")

# COMMAND ----------
# MAGIC %md ## 9. MERGE upsert

# COMMAND ----------
MERGE_KEY = "t.team_id = s.team_id AND t.match_id = s.match_id"

if spark.catalog.tableExists(FULL_TABLE):
    dt = DeltaTable.forName(spark, FULL_TABLE)
    dt.alias("t").merge(team_perf.alias("s"), MERGE_KEY) \
      .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"✅ MERGE: {FULL_TABLE}")
else:
    team_perf.write.format("delta").mode("overwrite") \
        .option("overwriteSchema","true") \
        .partitionBy("_competition_id") \
        .saveAsTable(FULL_TABLE)
    print(f"✅ Criada: {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md ## 10. Validação

# COMMAND ----------
tp = spark.table(FULL_TABLE)
print(f"Total: {tp.count():,} registros")

print("\n📊 PPDA médio por seleção (Copa 2022 — ordenado por pressing intensity):")
tp.filter(F.col("_competition_label") == "FIFA World Cup 2022") \
    .groupBy("team_name") \
    .agg(
        F.avg("ppda").alias("ppda_medio"),
        F.avg("possession_pct").alias("posse_media"),
        F.sum("xg").alias("xg_total"),
        F.sum("xga").alias("xga_total"),
        F.sum("goals_scored").alias("gols_marcados"),
    ) \
    .withColumn("xg_diff", F.round(F.col("xg_total") - F.col("xga_total"), 2)) \
    .orderBy("ppda_medio") \
    .show(20, truncate=False)

print("\n🔝 Times com maior xG diferencial (WC 2022):")
tp.filter(F.col("_competition_label") == "FIFA World Cup 2022") \
    .groupBy("team_name") \
    .agg(
        F.sum("xg").alias("xg"),
        F.sum("xga").alias("xga"),
        F.sum("goals_scored").alias("gols"),
        F.count("*").alias("jogos"),
    ) \
    .withColumn("xg_per_game", F.round(F.col("xg")/F.col("jogos"), 2)) \
    .withColumn("xga_per_game", F.round(F.col("xga")/F.col("jogos"), 2)) \
    .orderBy((F.col("xg")-F.col("xga")).desc()) \
    .show(15, truncate=False)
