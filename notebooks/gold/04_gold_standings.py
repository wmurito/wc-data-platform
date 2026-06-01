# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Standings (Classificação por torneio)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 04 Gold
# MAGIC **Tabela:** `worldcup.gold.standings`
# MAGIC **Fonte:** `worldcup.silver.team_performance` + `worldcup.bronze.statsbomb_matches`
# MAGIC **Executar depois de:** 03_silver_team_performance
# MAGIC
# MAGIC Classificação completa por torneio e fase:
# MAGIC pontos, saldo de gols, xG diferencial, e a fase máxima atingida.
# MAGIC Útil para análise de "quem performou acima/abaixo do esperado".

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_G   = "gold"
SCHEMA_B   = "bronze"
TABLE      = "standings"
FULL_TABLE = f"{CATALOG}.{SCHEMA_G}.{TABLE}"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_G}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_S   = "wc_silver"
    SCHEMA_G   = "wc_gold"
    SCHEMA_B   = "wc_bronze"
    FULL_TABLE = f"{CATALOG}.{SCHEMA_G}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_G}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

tp = spark.table(f"{CATALOG}.{SCHEMA_S}.team_performance")
matches = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_matches")

# COMMAND ----------
# MAGIC %md ## 1. Standings por torneio (total, todas as fases)

# COMMAND ----------
standings = tp.groupBy(
    "team_id","team_name",
    "_competition_id","_competition_label","_season_id"
).agg(
    F.count("*").alias("matches_played"),
    F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("wins"),
    F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("draws"),
    F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("losses"),
    F.sum("goals_scored").alias("goals_for"),
    F.sum("goals_conceded").alias("goals_against"),
    F.round(F.sum("xg"), 3).alias("xg_for"),
    F.round(F.sum("xga"), 3).alias("xga_against"),
    F.round(F.avg("possession_pct"), 1).alias("avg_possession"),
    F.round(F.avg("ppda"), 2).alias("avg_ppda"),
    F.round(F.avg("shots_total"), 1).alias("avg_shots"),
    F.sum("progressive_passes").alias("total_prog_passes"),
).withColumn(
    "points", F.col("wins") * 3 + F.col("draws")
).withColumn(
    "goal_diff", F.col("goals_for") - F.col("goals_against")
).withColumn(
    "xg_diff", F.round(F.col("xg_for") - F.col("xga_against"), 3)
).withColumn(
    "xg_points",   # pontos que o time "merecia" baseado em xG
    F.round(F.col("xg_for") / (F.col("xg_for") + F.col("xga_against")) * F.col("matches_played") * 3, 1)
).withColumn(
    "luck_index",  # pontos reais - pontos esperados por xG
    F.round(F.col("points") - F.col("xg_points"), 1)
)

# Ranking dentro de cada competição
w_rank = Window.partitionBy("_competition_id").orderBy(
    F.col("points").desc(),
    F.col("goal_diff").desc(),
    F.col("goals_for").desc(),
    F.col("xg_diff").desc(),
)
standings_ranked = standings.withColumn("rank_overall", F.rank().over(w_rank))

# COMMAND ----------
# MAGIC %md ## 2. Fase máxima atingida por cada time

# COMMAND ----------
stage_order_expr = F.when(F.col("stage_name").contains("Final") & ~F.col("stage_name").contains("3rd"), 7) \
    .when(F.col("stage_name").contains("3rd"), 6) \
    .when(F.col("stage_name").contains("Semi"), 5) \
    .when(F.col("stage_name").contains("Quarter"), 4) \
    .when(F.col("stage_name").contains("16"), 3) \
    .when(F.col("stage_name").contains("32"), 2) \
    .otherwise(1)

max_stage = tp.withColumn("stage_ordinal", stage_order_expr) \
    .groupBy("team_id","team_name","_competition_id") \
    .agg(F.max("stage_ordinal").alias("max_stage_ordinal")) \
    .withColumn("furthest_stage",
        F.when(F.col("max_stage_ordinal") == 7, "Final")
         .when(F.col("max_stage_ordinal") == 6, "3rd Place")
         .when(F.col("max_stage_ordinal") == 5, "Semi-finals")
         .when(F.col("max_stage_ordinal") == 4, "Quarter-finals")
         .when(F.col("max_stage_ordinal") == 3, "Round of 16")
         .when(F.col("max_stage_ordinal") == 2, "Round of 32")
         .otherwise("Group Stage")
    )

standings_final = standings_ranked.join(
    max_stage.select("team_id","_competition_id","furthest_stage","max_stage_ordinal"),
    on=["team_id","_competition_id"], how="left"
).withColumn("_processed_at", F.current_timestamp())

# COMMAND ----------
# MAGIC %md ## 3. Escrita

# COMMAND ----------
standings_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("_competition_id") \
    .saveAsTable(FULL_TABLE)
print(f"✅ {FULL_TABLE}: {standings_final.count():,} registros")

# COMMAND ----------
# MAGIC %md ## 4. Validação

# COMMAND ----------
st = spark.table(FULL_TABLE)

print("🏆 Classificação geral — Copa do Mundo 2022:")
st.filter(F.col("_competition_label") == "FIFA World Cup 2022") \
    .select(
        "rank_overall","team_name","furthest_stage",
        "matches_played","wins","draws","losses","points",
        "goals_for","goals_against","goal_diff",
        "xg_for","xga_against","xg_diff","luck_index",
        "avg_possession","avg_ppda"
    ).orderBy("rank_overall") \
    .show(32, truncate=False)

print("\n🍀 Times com mais 'sorte' (pontos reais muito acima do xG):")
st.filter(F.col("luck_index").isNotNull()) \
    .select("team_name","_competition_label","points","xg_points","luck_index","furthest_stage") \
    .orderBy(F.col("luck_index").desc()) \
    .show(15, truncate=False)

print("\n😤 Times 'azarados' (performaram melhor do que os pontos indicam):")
st.filter(F.col("luck_index").isNotNull()) \
    .select("team_name","_competition_label","points","xg_points","luck_index","furthest_stage") \
    .orderBy("luck_index") \
    .show(15, truncate=False)
