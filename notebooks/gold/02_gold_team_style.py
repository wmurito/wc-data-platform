# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Team Style Index
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 04 Gold
# MAGIC **Tabela:** `worldcup.gold.team_style`
# MAGIC **Fonte:** `worldcup.silver.team_performance`
# MAGIC **Executar depois de:** 03_silver_team_performance
# MAGIC
# MAGIC Consolida o estilo de jogo de cada seleção ao longo dos torneios.
# MAGIC Classifica automaticamente o perfil tático:
# MAGIC   - PRESSING ALTO (PPDA < 8, pressures_high% > 40%)
# MAGIC   - POSSE DOMINANTE (possession > 58%, progressive_passes > 25/jogo)
# MAGIC   - CONTRA-ATAQUE (possession < 45%, shots_total alto com posse baixa)
# MAGIC   - BLOCO BAIXO (ppda > 14, defensive_line_x < 45)
# MAGIC   - EQUILIBRADO (não se encaixa em nenhum extremo)
# MAGIC
# MAGIC Esta tabela é feature store para o ML (estilo home vs estilo away).

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "lakehouse"
SCHEMA_S   = "silver"
SCHEMA_G   = "gold"
TABLE      = "team_style"
FULL_TABLE = f"{CATALOG}.{SCHEMA_G}.{TABLE}"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_G}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_S   = "wc_silver"
    SCHEMA_G   = "wc_gold"
    FULL_TABLE = f"{CATALOG}.{SCHEMA_G}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_G}")

# COMMAND ----------
from pyspark.sql import functions as F

tp = spark.table(f"{CATALOG}.{SCHEMA_S}.team_performance")

# COMMAND ----------
# MAGIC %md ## 1. Médias por seleção por torneio

# COMMAND ----------
style_by_comp = tp.groupBy(
    "team_id", "team_name",
    "_competition_id", "_competition_label", "_season_id"
).agg(
    # Volume de jogos
    F.count("*").alias("matches_played"),
    F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("wins"),
    F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("draws"),
    F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("losses"),

    # Ataque
    F.round(F.avg("xg"), 3).alias("xg_per_game"),
    F.round(F.sum("xg"), 3).alias("xg_total"),
    F.round(F.avg("shots_total"), 2).alias("shots_per_game"),
    F.round(F.avg("shots_on_target"), 2).alias("shots_on_target_per_game"),
    F.round(F.avg("goals_scored"), 2).alias("goals_per_game"),
    F.round(F.avg("avg_shot_distance"), 2).alias("avg_shot_distance"),

    # Defesa
    F.round(F.avg("xga"), 3).alias("xga_per_game"),
    F.round(F.sum("xga"), 3).alias("xga_total"),
    F.round(F.avg("goals_conceded"), 2).alias("goals_conceded_per_game"),
    F.round(F.avg("clearances"), 2).alias("clearances_per_game"),
    F.round(F.avg("avg_defensive_line_x"), 2).alias("avg_defensive_line_x"),

    # Posse e progressão
    F.round(F.avg("possession_pct"), 2).alias("avg_possession_pct"),
    F.round(F.avg("progressive_passes"), 2).alias("progressive_passes_per_game"),
    F.round(F.avg("progressive_carries"), 2).alias("progressive_carries_per_game"),
    F.round(F.avg("avg_pass_length"), 2).alias("avg_pass_length"),
    F.round(F.avg("crosses"), 2).alias("crosses_per_game"),

    # Pressing
    F.round(F.avg("ppda"), 2).alias("avg_ppda"),

    # xG diferencial
    F.round(F.sum("xg") - F.sum("xga"), 3).alias("xg_diff_total"),
).withColumn(
    "xg_diff_per_game",
    F.round(F.col("xg_diff_total") / F.col("matches_played"), 3)
).withColumn(
    "points",
    F.col("wins") * 3 + F.col("draws")
).withColumn(
    "win_pct",
    F.round(F.col("wins") / F.col("matches_played") * 100, 1)
)

# COMMAND ----------
# MAGIC %md ## 2. Classificação tática automática

# COMMAND ----------
style_classified = style_by_comp.withColumn(
    "tactical_profile",
    F.when(
        (F.col("avg_ppda") < 8) &
        (F.col("avg_possession_pct") > 50),
        F.lit("PRESSING_ALTO")
    ).when(
        (F.col("avg_possession_pct") >= 58) &
        (F.col("progressive_passes_per_game") >= 20),
        F.lit("POSSE_DOMINANTE")
    ).when(
        (F.col("avg_possession_pct") < 45) &
        (F.col("xg_per_game") >= 1.2),
        F.lit("CONTRA_ATAQUE")
    ).when(
        (F.col("avg_ppda") > 14) &
        (F.col("avg_defensive_line_x").isNotNull()) &
        (F.col("avg_defensive_line_x") < 45),
        F.lit("BLOCO_BAIXO")
    ).otherwise(
        F.lit("EQUILIBRADO")
    )
).withColumn(
    # Score composto de dominância (útil como feature ML)
    "dominance_score",
    F.round(
        F.col("xg_diff_per_game") * 0.4 +
        (F.col("avg_possession_pct") - 50) * 0.02 +
        F.when(F.col("avg_ppda").isNotNull(), (20 - F.col("avg_ppda")) * 0.03)
         .otherwise(F.lit(0)),
        3
    )
)

# COMMAND ----------
# MAGIC %md ## 3. Médias globais por seleção (todos torneios combinados)

# COMMAND ----------
style_overall = tp.groupBy("team_id","team_name").agg(
    F.count("*").alias("total_matches"),
    F.round(F.avg("xg"), 3).alias("overall_xg_per_game"),
    F.round(F.avg("xga"), 3).alias("overall_xga_per_game"),
    F.round(F.avg("ppda"), 2).alias("overall_ppda"),
    F.round(F.avg("possession_pct"), 2).alias("overall_possession_pct"),
    F.round(F.avg("progressive_passes"), 2).alias("overall_prog_passes"),
    F.round(F.avg("avg_defensive_line_x"), 2).alias("overall_defensive_line_x"),
    F.round(F.sum("xg") - F.sum("xga"), 3).alias("overall_xg_diff"),
    F.collect_set("_competition_label").alias("competitions_included"),
).withColumn(
    "overall_dominance_score",
    F.round(
        (F.col("overall_xg_per_game") - F.col("overall_xga_per_game")) * 0.4 +
        (F.col("overall_possession_pct") - 50) * 0.02 +
        F.when(F.col("overall_ppda").isNotNull(), (20 - F.col("overall_ppda")) * 0.03)
         .otherwise(F.lit(0)),
        3
    )
)

# COMMAND ----------
# MAGIC %md ## 4. Escrita Gold

# COMMAND ----------
style_final = style_classified.withColumn("_processed_at", F.current_timestamp())
style_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_competition_id") \
    .saveAsTable(FULL_TABLE)
print(f"✅ {FULL_TABLE}: {style_final.count():,} registros")

overall_final = style_overall.withColumn("_processed_at", F.current_timestamp())
overall_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_G}.team_style_overall")
print(f"✅ team_style_overall: {overall_final.count():,} seleções")

# COMMAND ----------
# MAGIC %md ## 5. Validação

# COMMAND ----------
ts = spark.table(FULL_TABLE)

print("🎯 Perfis táticos — Copa 2022:")
ts.filter(F.col("_competition_label") == "FIFA World Cup 2022") \
    .select(
        "team_name","tactical_profile","matches_played",
        "avg_possession_pct","avg_ppda","xg_per_game","xga_per_game",
        "xg_diff_per_game","dominance_score","win_pct"
    ).orderBy("dominance_score", ascending=False) \
    .show(20, truncate=False)

print("\n📊 Distribuição de perfis táticos por competição:")
ts.groupBy("_competition_label","tactical_profile").count() \
    .orderBy("_competition_label","count", ascending=False) \
    .show(30, truncate=False)

print("\n🏆 Times mais dominantes (todos torneios):")
spark.table(f"{CATALOG}.{SCHEMA_G}.team_style_overall") \
    .orderBy("overall_dominance_score", ascending=False) \
    .select(
        "team_name","total_matches",
        "overall_xg_per_game","overall_xga_per_game",
        "overall_ppda","overall_possession_pct","overall_dominance_score"
    ).show(20, truncate=False)
