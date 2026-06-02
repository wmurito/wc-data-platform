# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Top Scorers & Artilharia
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 04 Gold
# MAGIC **Tabela:** `worldcup.gold.top_scorers`
# MAGIC **Fonte:** `worldcup.silver.player_match_stats`
# MAGIC **Executar depois de:** 02_silver_player_stats
# MAGIC
# MAGIC Ranking de artilheiros por torneio com métricas completas:
# MAGIC gols reais, xG acumulado, eficiência (gols/xG), assistências,
# MAGIC gols por 90min, tipo de gol (cabeça, perna, pênalti).
# MAGIC
# MAGIC Também gera ranking cross-torneio (quem foi melhor nos últimos
# MAGIC 4 torneios combinados — WC 2018 + WC 2022 + Euro 2020 + Euro 2024).

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "lakehouse"
SCHEMA_S   = "silver"
SCHEMA_G   = "gold"
TABLE      = "top_scorers"
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
from pyspark.sql.window import Window

ps = spark.table(f"{CATALOG}.{SCHEMA_S}.player_match_stats")

# COMMAND ----------
# MAGIC %md ## 1. Artilharia por torneio

# COMMAND ----------
# Agregação por jogador + torneio
scorers_by_comp = ps.groupBy(
    "player_id", "player_name", "team_name",
    "_competition_id", "_competition_label", "_season_id",
    "position_name", "country_name"
).agg(
    # Gols e xG
    F.sum("goals").alias("goals"),
    F.round(F.sum("xg_total"), 3).alias("xg_total"),
    F.round(F.sum("xg_total") - F.sum("goals"), 3).alias("goals_minus_xg"),

    # Assistências e criação
    F.sum("assists").alias("assists"),
    F.sum("xa_total").alias("xa_total"),
    F.sum("key_passes").alias("key_passes"),

    # Volume de chutes
    F.sum("shots_total").alias("shots_total"),
    F.sum("shots_on_target").alias("shots_on_target"),

    # Tipos de gol
    F.sum("shots_head").alias("goals_head"),           # headers (approx)
    F.sum("penalties_scored").alias("penalties_scored"),
    F.sum("penalties_taken").alias("penalties_taken"),

    # Minutagem estimada
    F.sum("last_minute_seen").alias("minutes_approx"),
    F.countDistinct("match_id").alias("matches_played"),
).withColumn(
    "goals_per_90",
    F.when(F.col("minutes_approx") > 0,
        F.round(F.col("goals") / F.col("minutes_approx") * 90, 3)
    ).otherwise(F.lit(None))
).withColumn(
    "xg_per_90",
    F.when(F.col("minutes_approx") > 0,
        F.round(F.col("xg_total") / F.col("minutes_approx") * 90, 3)
    ).otherwise(F.lit(None))
).withColumn(
    "shot_conversion_pct",
    F.when(F.col("shots_total") > 0,
        F.round(F.col("goals") / F.col("shots_total") * 100, 1)
    ).otherwise(F.lit(None))
).withColumn(
    "shots_on_target_pct",
    F.when(F.col("shots_total") > 0,
        F.round(F.col("shots_on_target") / F.col("shots_total") * 100, 1)
    ).otherwise(F.lit(None))
)

# Ranking dentro de cada torneio
w_comp = Window.partitionBy("_competition_id").orderBy(
    F.col("goals").desc(), F.col("xg_total").desc()
)
scorers_ranked = scorers_by_comp.withColumn(
    "rank_in_competition", F.rank().over(w_comp)
)

print(f"Artilheiros por torneio: {scorers_ranked.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Ranking cross-torneio (últimos 4 torneios)

# COMMAND ----------
cross_tournament = ps.groupBy("player_id","player_name","team_name","position_name") \
    .agg(
        F.sum("goals").alias("goals_total"),
        F.round(F.sum("xg_total"), 3).alias("xg_total"),
        F.sum("assists").alias("assists_total"),
        F.countDistinct("match_id").alias("total_matches"),
        F.countDistinct("_competition_id").alias("tournaments_played"),
        F.collect_set("_competition_label").alias("competitions"),
    ).withColumn(
        "goal_contributions", F.col("goals_total") + F.col("assists_total")
    ).withColumn(
        "goals_per_match",
        F.round(F.col("goals_total") / F.col("total_matches"), 3)
    ).withColumn(
        "xg_per_match",
        F.round(F.col("xg_total") / F.col("total_matches"), 3)
    )

# COMMAND ----------
# MAGIC %md ## 3. Escrita Gold (overwrite — sempre recalculada completa)

# COMMAND ----------
scorers_final = scorers_ranked.withColumn(
    "_processed_at", F.current_timestamp()
)

scorers_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_competition_id") \
    .saveAsTable(FULL_TABLE)
print(f"✅ {FULL_TABLE}: {scorers_final.count():,} registros")

# Cross-tournament
cross_final = cross_tournament.withColumn("_processed_at", F.current_timestamp())
cross_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_G}.top_scorers_cross_tournament")
print(f"✅ top_scorers_cross_tournament: {cross_final.count():,} jogadores")

# COMMAND ----------
# MAGIC %md ## 4. Validação — os números devem bater com a realidade

# COMMAND ----------
ts = spark.table(FULL_TABLE)

for comp_label, expected_goals, expected_top_scorer in [
    ("FIFA World Cup 2022", 172, "Kylian Mbappé"),
    ("FIFA World Cup 2018", 169, "Harry Kane"),
    ("UEFA Euro 2024", None, None),
]:
    total = ts.filter(F.col("_competition_label") == comp_label) \
              .agg(F.sum("goals")).collect()[0][0]
    print(f"\n{comp_label}:")
    print(f"  Gols totais: {total} {'✅' if expected_goals is None or total == expected_goals else f'⚠️ esperado {expected_goals}'}")

    print(f"  Top 10 artilheiros:")
    ts.filter(
        (F.col("_competition_label") == comp_label) &
        (F.col("goals") >= 1)
    ).select(
        "rank_in_competition","player_name","team_name",
        "goals","xg_total","goals_minus_xg",
        "assists","shots_total","shot_conversion_pct","goals_per_90"
    ).orderBy("rank_in_competition").show(10, truncate=False)

print("\n🌍 Top 15 cross-torneio (gols + assists):")
spark.table(f"{CATALOG}.{SCHEMA_G}.top_scorers_cross_tournament") \
    .filter(F.col("goals_total") >= 3) \
    .select(
        "player_name","team_name","tournaments_played",
        "total_matches","goals_total","assists_total",
        "goal_contributions","xg_total","goals_per_match"
    ).orderBy("goal_contributions", ascending=False) \
    .show(15, truncate=False)
