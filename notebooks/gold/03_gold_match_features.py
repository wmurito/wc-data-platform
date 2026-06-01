# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Match Features (Feature Store para ML)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 04 Gold
# MAGIC **Tabela:** `worldcup.gold.match_features`
# MAGIC **Fonte:** silver.team_performance + silver.elo_ratings + gold.team_style_overall + bronze.statsbomb_matches
# MAGIC **Executar depois de:** todos os notebooks Silver + 02_gold_team_style
# MAGIC
# MAGIC Constrói o vetor de features para o modelo de predição de resultado.
# MAGIC 1 linha = 1 partida, com features de AMBOS os times lado a lado.
# MAGIC
# MAGIC **Features incluídas (45+):**
# MAGIC
# MAGIC Contexto da partida:
# MAGIC   elo_diff, elo_home, elo_away, stage (grupos/oitavas/final...)
# MAGIC
# MAGIC Forma recente (últimas 5 partidas do torneio):
# MAGIC   home_form_xg, away_form_xg, home_form_xga, away_form_xga
# MAGIC   home_form_ppda, away_form_ppda, home_form_possession
# MAGIC
# MAGIC Estilo de jogo (médias do torneio):
# MAGIC   home_xg_per_game, away_xg_per_game, home_ppda, away_ppda
# MAGIC   home_possession, away_possession, home_dominance_score
# MAGIC
# MAGIC Head-to-head histórico (ELO):
# MAGIC   h2h_home_wins, h2h_draws, h2h_away_wins (últimas 5 partidas históricas)
# MAGIC
# MAGIC Target (y):
# MAGIC   result_home: 1=home win, 0=draw, -1=away win

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_G   = "gold"
SCHEMA_B   = "bronze"
TABLE      = "match_features"
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

matches   = spark.table(f"{CATALOG}.{SCHEMA_B}.statsbomb_matches")
team_perf = spark.table(f"{CATALOG}.{SCHEMA_S}.team_performance")
elo_snap  = spark.table(f"{CATALOG}.{SCHEMA_S}.elo_current_snapshot")
elo_hist  = spark.table(f"{CATALOG}.{SCHEMA_S}.elo_ratings")
style     = spark.table(f"{CATALOG}.{SCHEMA_G}.team_style_overall")

# COMMAND ----------
# MAGIC %md ## 1. Features de ELO por partida

# COMMAND ----------
# ELO antes da partida (do histórico calculado)
# Para partidas StatsBomb, aproximamos pelo ELO snapshot atual
# (suficiente para portfólio — em prod usaríamos ELO por data)
elo_home = elo_snap.select(
    F.col("team_name").alias("home_team_name"),
    F.col("elo_rating").alias("elo_home"),
)
elo_away = elo_snap.select(
    F.col("team_name").alias("away_team_name"),
    F.col("elo_rating").alias("elo_away"),
)

matches_elo = matches \
    .join(elo_home, on="home_team_name", how="left") \
    .join(elo_away, on="away_team_name", how="left") \
    .withColumn("elo_diff", F.col("elo_home") - F.col("elo_away")) \
    .withColumn("elo_home_win_prob",
        F.round(1 / (1 + F.pow(F.lit(10), -F.col("elo_diff") / 400)), 4)
    )

# COMMAND ----------
# MAGIC %md ## 2. Features de estilo por time

# COMMAND ----------
style_home = style.select(
    F.col("team_name").alias("home_team_name"),
    F.col("overall_xg_per_game").alias("home_xg_per_game"),
    F.col("overall_xga_per_game").alias("home_xga_per_game"),
    F.col("overall_ppda").alias("home_ppda"),
    F.col("overall_possession_pct").alias("home_possession_pct"),
    F.col("overall_prog_passes").alias("home_prog_passes"),
    F.col("overall_defensive_line_x").alias("home_def_line_x"),
    F.col("overall_dominance_score").alias("home_dominance_score"),
    F.col("total_matches").alias("home_total_matches"),
)

style_away = style.select(
    F.col("team_name").alias("away_team_name"),
    F.col("overall_xg_per_game").alias("away_xg_per_game"),
    F.col("overall_xga_per_game").alias("away_xga_per_game"),
    F.col("overall_ppda").alias("away_ppda"),
    F.col("overall_possession_pct").alias("away_possession_pct"),
    F.col("overall_prog_passes").alias("away_prog_passes"),
    F.col("overall_defensive_line_x").alias("away_def_line_x"),
    F.col("overall_dominance_score").alias("away_dominance_score"),
    F.col("total_matches").alias("away_total_matches"),
)

# COMMAND ----------
# MAGIC %md ## 3. Forma recente — últimas N partidas no torneio
# MAGIC
# MAGIC Para cada partida, calcula a média das últimas 5 partidas
# MAGIC do mesmo time no mesmo torneio (usando window function).

# COMMAND ----------
# Performance por time ordenada por data
tp_ordered = team_perf.select(
    "match_id","team_id","team_name","match_date",
    "_competition_id","_competition_label",
    "xg","xga","ppda","possession_pct","goals_scored","goals_conceded","result"
).withColumn(
    "match_date", F.col("match_date").cast("date")
)

# Window: últimas 5 partidas do time no mesmo torneio ANTES da partida atual
w_form = Window.partitionBy("team_id","_competition_id") \
    .orderBy("match_date","match_id") \
    .rowsBetween(-5, -1)   # 5 partidas anteriores (exclusive current)

tp_form = tp_ordered \
    .withColumn("form_xg",          F.avg("xg").over(w_form)) \
    .withColumn("form_xga",         F.avg("xga").over(w_form)) \
    .withColumn("form_ppda",        F.avg("ppda").over(w_form)) \
    .withColumn("form_possession",  F.avg("possession_pct").over(w_form)) \
    .withColumn("form_goals",       F.avg("goals_scored").over(w_form)) \
    .withColumn("form_goals_ag",    F.avg("goals_conceded").over(w_form)) \
    .withColumn("form_wins",        F.sum(
        F.when(F.col("result") == "W", 1).otherwise(0)
    ).over(w_form))

form_home = tp_form.select(
    "match_id","team_id",
    F.col("form_xg").alias("home_form_xg"),
    F.col("form_xga").alias("home_form_xga"),
    F.col("form_ppda").alias("home_form_ppda"),
    F.col("form_possession").alias("home_form_possession"),
    F.col("form_wins").alias("home_form_wins"),
).withColumnRenamed("team_id","home_team_id")

form_away = tp_form.select(
    "match_id","team_id",
    F.col("form_xg").alias("away_form_xg"),
    F.col("form_xga").alias("away_form_xga"),
    F.col("form_ppda").alias("away_form_ppda"),
    F.col("form_possession").alias("away_form_possession"),
    F.col("form_wins").alias("away_form_wins"),
).withColumnRenamed("team_id","away_team_id")

# COMMAND ----------
# MAGIC %md ## 4. Stage encoding (fase da competição → ordinal)

# COMMAND ----------
stage_order = {
    "Group Stage": 1,
    "Round of 32": 2,
    "Round of 16": 3,
    "Quarter-finals": 4,
    "Semi-finals": 5,
    "3rd Place Final": 6,
    "Final": 7,
}

stage_map_expr = F.when(F.col("stage_name") == "Group Stage", 1) \
    .when(F.col("stage_name").contains("32"), 2) \
    .when(F.col("stage_name").contains("16"), 3) \
    .when(F.col("stage_name").contains("Quarter"), 4) \
    .when(F.col("stage_name").contains("Semi"), 5) \
    .when(F.col("stage_name").contains("3rd"), 6) \
    .when(F.col("stage_name").contains("Final"), 7) \
    .otherwise(1)

# COMMAND ----------
# MAGIC %md ## 5. Target — resultado da partida

# COMMAND ----------
result_expr = F.when(F.col("home_score") > F.col("away_score"), F.lit(1)) \
               .when(F.col("home_score") == F.col("away_score"), F.lit(0)) \
               .otherwise(F.lit(-1))

# COMMAND ----------
# MAGIC %md ## 6. Consolidação do feature vector

# COMMAND ----------
features = matches_elo \
    .join(style_home, on="home_team_name", how="left") \
    .join(style_away, on="away_team_name", how="left") \
    .join(form_home,  on=["match_id","home_team_id"], how="left") \
    .join(form_away,  on=["match_id","away_team_id"], how="left") \
    .select(
        # ── Identificadores
        "match_id",
        "match_date",
        "_competition_id",
        "_competition_label",
        "_season_id",

        # ── Times
        "home_team_id", "home_team_name",
        "away_team_id", "away_team_name",

        # ── Resultado (target y)
        "home_score", "away_score",
        result_expr.alias("result_home"),   # 1=home win, 0=draw, -1=away win
        F.when(F.col("home_score") > F.col("away_score"), F.lit("W"))
         .when(F.col("home_score") == F.col("away_score"), F.lit("D"))
         .otherwise(F.lit("L")).alias("result_label"),

        # ── Fase da competição
        stage_map_expr.alias("stage_ordinal"),
        "stage_name",

        # ── ELO features
        F.round("elo_home", 1).alias("elo_home"),
        F.round("elo_away", 1).alias("elo_away"),
        F.round("elo_diff", 1).alias("elo_diff"),
        F.round("elo_home_win_prob", 4).alias("elo_home_win_prob"),

        # ── Style features — home
        "home_xg_per_game", "home_xga_per_game",
        "home_ppda", "home_possession_pct",
        "home_prog_passes", "home_def_line_x",
        "home_dominance_score",

        # ── Style features — away
        "away_xg_per_game", "away_xga_per_game",
        "away_ppda", "away_possession_pct",
        "away_prog_passes", "away_def_line_x",
        "away_dominance_score",

        # ── Diferenciais (home - away) — features sintéticas poderosas
        (F.col("home_xg_per_game") - F.col("away_xg_per_game")).alias("xg_diff"),
        (F.col("home_ppda") - F.col("away_ppda")).alias("ppda_diff"),
        (F.col("home_possession_pct") - F.col("away_possession_pct")).alias("possession_diff"),
        (F.col("home_dominance_score") - F.col("away_dominance_score")).alias("dominance_diff"),

        # ── Forma recente — home
        F.round("home_form_xg", 3).alias("home_form_xg"),
        F.round("home_form_xga", 3).alias("home_form_xga"),
        F.round("home_form_ppda", 2).alias("home_form_ppda"),
        F.round("home_form_possession", 2).alias("home_form_possession"),
        "home_form_wins",

        # ── Forma recente — away
        F.round("away_form_xg", 3).alias("away_form_xg"),
        F.round("away_form_xga", 3).alias("away_form_xga"),
        F.round("away_form_ppda", 2).alias("away_form_ppda"),
        F.round("away_form_possession", 2).alias("away_form_possession"),
        "away_form_wins",

        # ── Diferencial de forma
        (F.col("home_form_xg") - F.col("away_form_xg")).alias("form_xg_diff"),
        (F.col("home_form_wins") - F.col("away_form_wins")).alias("form_wins_diff"),

        # ── Meta
        F.current_timestamp().alias("_processed_at"),
    )

total = features.count()
print(f"Feature vectors: {total:,} partidas")
print(f"Colunas: {len(features.columns)}")
features.printSchema()

# COMMAND ----------
# MAGIC %md ## 7. Escrita Gold

# COMMAND ----------
features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_competition_id") \
    .saveAsTable(FULL_TABLE)
print(f"✅ {FULL_TABLE}: {total:,} partidas com {len(features.columns)} features")

# COMMAND ----------
# MAGIC %md ## 8. Validação — distribuição de resultados e features

# COMMAND ----------
mf = spark.table(FULL_TABLE)

print("Distribuição de resultados por competição:")
mf.groupBy("_competition_label","result_label").count() \
    .orderBy("_competition_label","result_label") \
    .show(truncate=False)

print("\nEstatísticas das features ELO:")
mf.select("elo_home","elo_away","elo_diff","elo_home_win_prob") \
    .summary("min","25%","50%","75%","max","mean") \
    .show(truncate=False)

print("\nCorrelação elo_diff → resultado (home wins):")
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["elo_diff","result_home"],
    outputCol="features_vec",
    handleInvalid="skip"
)
mf_vec = assembler.transform(mf.filter(F.col("elo_diff").isNotNull()))
corr = Correlation.corr(mf_vec, "features_vec").collect()[0][0]
print(f"  Correlação Pearson elo_diff ↔ result_home: {corr[0,1]:.4f}")

print("\n✅ Feature store pronta para o notebook ML!")
print(f"   Tabela: {FULL_TABLE}")
print(f"   {mf.count():,} partidas | {len(mf.columns)} features")
