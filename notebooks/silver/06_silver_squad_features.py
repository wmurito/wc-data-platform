# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Squad Features (Qualidade de Elenco)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 03 Silver — Enriquecimento
# MAGIC **Tabela:** `worldcup.silver.squad_features`
# MAGIC **Fonte:** DBFS enrichment/ (SoFIFA + FBref forma + Transfermarkt)
# MAGIC **Executar depois de:** 04_squad_enrichment_scraper.py + upload DBFS
# MAGIC
# MAGIC Calcula métricas de qualidade de elenco por seleção:
# MAGIC
# MAGIC  RATINGS (SoFIFA):
# MAGIC    squad_avg_overall, squad_top11_overall, squad_depth_score
# MAGIC    squad_avg_age, squad_peak_pct (% jogadores 25-29 anos)
# MAGIC    squad_avg_potential, squad_overall_std (homogeneidade)
# MAGIC    pos_gk_rating, pos_def_rating, pos_mid_rating, pos_att_rating
# MAGIC    key_players_avg_overall (top 3 por posição)
# MAGIC
# MAGIC  FORMA RECENTE (últimas 10 partidas):
# MAGIC    form_ppg, form_xg_per_game, form_xga_per_game
# MAGIC    form_xg_diff, form_win_rate, form_clean_sheets_pct
# MAGIC    form_goals_per_game, form_goals_against_per_game
# MAGIC
# MAGIC  VALOR DE MERCADO:
# MAGIC    squad_value_m, squad_value_log (normalizado)
# MAGIC    top3_value_m, depth_value_ratio

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
DBFS_ENRICH = "dbfs:/FileStore/wc-platform/raw/enrichment"
CATALOG      = "worldcup"
SCHEMA_S     = "silver"
TABLE        = "squad_features"
FULL_TABLE   = f"{CATALOG}.{SCHEMA_S}.{TABLE}"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_S}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_S   = "wc_silver"
    FULL_TABLE = f"{CATALOG}.{SCHEMA_S}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA_S}")

# COMMAND ----------
# MAGIC %md ## 1. Carregar dados brutos

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import math

# ── SoFIFA ratings
try:
    sofifa_raw = spark.read.parquet(f"{DBFS_ENRICH}/sofifa_squad_ratings.parquet")
    print(f"SoFIFA: {sofifa_raw.count():,} jogadores")
    sofifa_raw.printSchema()
except Exception as e:
    print(f"⚠️  SoFIFA não encontrado: {e}")
    sofifa_raw = None

# ── Forma recente
try:
    form_raw = spark.read.parquet(f"{DBFS_ENRICH}/national_team_form.parquet")
    print(f"Forma: {form_raw.count():,} jogos")
except Exception as e:
    print(f"⚠️  Forma não encontrada: {e}")
    form_raw = None

# ── Valor de mercado
try:
    market_raw = spark.read.parquet(f"{DBFS_ENRICH}/squad_market_values.parquet")
    print(f"Market: {market_raw.count():,} seleções")
except Exception as e:
    print(f"⚠️  Market não encontrado: {e}")
    market_raw = None

# COMMAND ----------
# MAGIC %md ## 2. Features de Rating (SoFIFA)

# COMMAND ----------
if sofifa_raw is not None:

    # Classificar posição em categoria
    sofifa = sofifa_raw.withColumn(
        "pos_category",
        F.when(F.col("position").contains("GK"), "GK")
         .when(F.col("position").rlike("CB|RB|LB|WB|RWB|LWB|DEF"), "DEF")
         .when(F.col("position").rlike("CDM|CM|CAM|RM|LM|MID"), "MID")
         .when(F.col("position").rlike("RW|LW|CF|ST|ATT"), "ATT")
         .otherwise("MID")
    ).withColumn("overall", F.col("overall").cast(IntegerType())) \
     .withColumn("age",     F.col("age").cast(IntegerType())) \
     .withColumn("potential", F.col("potential").cast(IntegerType()))

    # ── Agregações por seleção
    rating_agg = sofifa.groupBy("wc_team_name").agg(
        # Rating médio e distribuição
        F.avg("overall").alias("squad_avg_overall"),
        F.stddev("overall").alias("squad_overall_std"),
        F.max("overall").alias("squad_best_player_overall"),
        F.avg("potential").alias("squad_avg_potential"),
        F.count("*").alias("squad_size"),

        # Idade
        F.avg("age").alias("squad_avg_age"),
        F.min("age").alias("squad_youngest"),
        F.max("age").alias("squad_oldest"),

        # % jogadores em peak age (25-29)
        F.sum(F.when((F.col("age") >= 25) & (F.col("age") <= 29), 1).otherwise(0))
         .alias("n_peak_age"),

        # % jogadores experientes (>30)
        F.sum(F.when(F.col("age") > 30, 1).otherwise(0))
         .alias("n_veterans"),

        # % jogadores jovens (<23)
        F.sum(F.when(F.col("age") < 23, 1).otherwise(0))
         .alias("n_youngsters"),
    ).withColumn(
        "squad_peak_pct",
        F.round(F.col("n_peak_age") / F.col("squad_size") * 100, 1)
    ).withColumn(
        "squad_veteran_pct",
        F.round(F.col("n_veterans") / F.col("squad_size") * 100, 1)
    ).withColumn(
        "squad_young_pct",
        F.round(F.col("n_youngsters") / F.col("squad_size") * 100, 1)
    )

    # ── Rating por posição
    pos_ratings = sofifa.groupBy("wc_team_name", "pos_category").agg(
        F.avg("overall").alias("pos_avg_overall"),
        F.max("overall").alias("pos_best_overall"),
    )

    # Pivotar posições
    gk_rating  = pos_ratings.filter(F.col("pos_category") == "GK")  \
        .select("wc_team_name", F.col("pos_avg_overall").alias("gk_avg_overall"),
                F.col("pos_best_overall").alias("gk_best_overall"))
    def_rating = pos_ratings.filter(F.col("pos_category") == "DEF") \
        .select("wc_team_name", F.col("pos_avg_overall").alias("def_avg_overall"),
                F.col("pos_best_overall").alias("def_best_overall"))
    mid_rating = pos_ratings.filter(F.col("pos_category") == "MID") \
        .select("wc_team_name", F.col("pos_avg_overall").alias("mid_avg_overall"),
                F.col("pos_best_overall").alias("mid_best_overall"))
    att_rating = pos_ratings.filter(F.col("pos_category") == "ATT") \
        .select("wc_team_name", F.col("pos_avg_overall").alias("att_avg_overall"),
                F.col("pos_best_overall").alias("att_best_overall"))

    # ── Top 11 (melhores 11 jogadores = titulares estimados)
    from pyspark.sql.window import Window
    w_top11 = Window.partitionBy("wc_team_name").orderBy(F.col("overall").desc())
    sofifa_ranked = sofifa.withColumn("player_rank", F.rank().over(w_top11))

    top11_rating = sofifa_ranked.filter(F.col("player_rank") <= 11) \
        .groupBy("wc_team_name") \
        .agg(F.avg("overall").alias("squad_top11_overall"),
             F.avg("age").alias("squad_top11_avg_age"))

    # Depth score = diferença entre top 11 e reservas
    bench_rating = sofifa_ranked.filter(F.col("player_rank") > 11) \
        .groupBy("wc_team_name") \
        .agg(F.avg("overall").alias("squad_bench_overall"))

    # ── Consolidar ratings
    squad_ratings = rating_agg \
        .join(gk_rating,  on="wc_team_name", how="left") \
        .join(def_rating, on="wc_team_name", how="left") \
        .join(mid_rating, on="wc_team_name", how="left") \
        .join(att_rating, on="wc_team_name", how="left") \
        .join(top11_rating, on="wc_team_name", how="left") \
        .join(bench_rating, on="wc_team_name", how="left") \
        .withColumn(
            "squad_depth_score",
            F.when(F.col("squad_bench_overall").isNotNull(),
                F.round(F.col("squad_bench_overall") / F.col("squad_top11_overall"), 3)
            ).otherwise(F.lit(0.90))
        ).withColumnRenamed("wc_team_name", "team_name")

    print(f"Rating features: {squad_ratings.count()} seleções")
    squad_ratings.select("team_name","squad_avg_overall","squad_top11_overall",
                          "squad_avg_age","squad_peak_pct","att_best_overall").show(10, truncate=False)

else:
    squad_ratings = None
    print("⚠️  Usando ratings padrão — SoFIFA não disponível")

# COMMAND ----------
# MAGIC %md ## 3. Features de Forma Recente

# COMMAND ----------
if form_raw is not None:

    # Ordenar por data e pegar últimas 10 por time
    from pyspark.sql.window import Window

    form = form_raw.withColumn("date", F.to_date("date")) \
                   .withColumn("goals_for",     F.col("goals_for").cast(IntegerType())) \
                   .withColumn("goals_against", F.col("goals_against").cast(IntegerType())) \
                   .withColumn("xg_for",        F.col("xg_for").cast(DoubleType())) \
                   .withColumn("xg_against",    F.col("xg_against").cast(DoubleType()))

    # Pontos por resultado
    form = form.withColumn("points",
        F.when(F.col("result") == "W", 3)
         .when(F.col("result") == "D", 1)
         .otherwise(0)
    ).withColumn("clean_sheet",
        F.when(F.col("goals_against") == 0, 1).otherwise(0)
    ).withColumn("xg_diff",
        F.col("xg_for") - F.col("xg_against")
    )

    # Window: últimas 10 por time
    w_form = Window.partitionBy("team").orderBy(F.col("date").desc())
    form_top10 = form.withColumn("match_rank", F.row_number().over(w_form)) \
                     .filter(F.col("match_rank") <= 10)

    form_agg = form_top10.groupBy("team").agg(
        F.count("*").alias("form_n_matches"),
        F.avg("points").alias("form_ppg"),
        F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("form_wins"),
        F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("form_draws"),
        F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("form_losses"),
        F.avg("xg_for").alias("form_xg_per_game"),
        F.avg("xg_against").alias("form_xga_per_game"),
        F.avg("xg_diff").alias("form_xg_diff"),
        F.avg("goals_for").alias("form_goals_per_game"),
        F.avg("goals_against").alias("form_goals_ag_per_game"),
        F.avg("clean_sheet").alias("form_clean_sheet_pct"),
        F.sum("goals_for").alias("form_total_goals"),
        F.sum("goals_against").alias("form_total_goals_ag"),
    ).withColumn(
        "form_win_rate",
        F.round(F.col("form_wins") / F.col("form_n_matches"), 3)
    ).withColumn(
        "form_ppg_rounded",
        F.round(F.col("form_ppg"), 2)
    ).withColumnRenamed("team", "team_name")

    print(f"Forma features: {form_agg.count()} seleções")
    form_agg.select("team_name","form_ppg","form_xg_per_game",
                     "form_xga_per_game","form_xg_diff","form_win_rate").show(10, truncate=False)

else:
    form_agg = None
    print("⚠️  Dados de forma não disponíveis")

# COMMAND ----------
# MAGIC %md ## 4. Features de Valor de Mercado

# COMMAND ----------
if market_raw is not None:

    market = market_raw \
        .withColumn("squad_value_m",       F.col("total_squad_value_m").cast(DoubleType())) \
        .withColumn("squad_value_log",     F.log(F.col("total_squad_value_m") + 1)) \
        .withColumn("avg_player_value_m",  F.col("avg_player_value_m").cast(DoubleType())) \
        .withColumn("top3_value_m",        F.col("top3_value_m").cast(DoubleType())) \
        .withColumn("depth_value_ratio",
            F.round(
                (F.col("total_squad_value_m") - F.col("top3_value_m")) /
                F.col("total_squad_value_m"), 3
            )
        ) \
        .withColumnRenamed("team", "team_name") \
        .select("team_name","squad_value_m","squad_value_log",
                "avg_player_value_m","top3_value_m",
                "n_players_over_50m","depth_value_ratio")

    print(f"Market features: {market.count()} seleções")
    market.orderBy("squad_value_m", ascending=False).show(10, truncate=False)

else:
    market = None
    print("⚠️  Dados de mercado não disponíveis")

# COMMAND ----------
# MAGIC %md ## 5. Consolidação — squad_features

# COMMAND ----------
# Base: todos os times que aparecem em qualquer fonte
all_teams_dfs = [
    df.select("team_name") for df in [squad_ratings, form_agg, market] if df is not None
]

from functools import reduce
from pyspark.sql import DataFrame

all_teams = reduce(
    lambda a, b: a.union(b), all_teams_dfs
).distinct()

# Join progressivo
squad_features = all_teams

if squad_ratings is not None:
    squad_features = squad_features.join(squad_ratings, on="team_name", how="left")

if form_agg is not None:
    squad_features = squad_features.join(form_agg, on="team_name", how="left")

if market is not None:
    squad_features = squad_features.join(market, on="team_name", how="left")

# Preencher nulos com medianas do dataset (evitar NaN no ML)
numeric_cols = [f.name for f in squad_features.schema.fields
                if str(f.dataType) in ("DoubleType", "IntegerType", "LongType")]

medians = squad_features.select(
    *[F.percentile_approx(c, 0.5).alias(c) for c in numeric_cols]
).collect()[0].asDict()

fill_map = {c: v for c, v in medians.items() if v is not None}
squad_features = squad_features.fillna(fill_map)

squad_features = squad_features.withColumn("_processed_at", F.current_timestamp())

total = squad_features.count()
print(f"\nSquad features consolidadas: {total} seleções | {len(squad_features.columns)} features")

# COMMAND ----------
# MAGIC %md ## 6. Escrita Delta

# COMMAND ----------
squad_features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FULL_TABLE)

print(f"✅ {FULL_TABLE}: {total} seleções")

# COMMAND ----------
# MAGIC %md ## 7. Validação

# COMMAND ----------
sf = spark.table(FULL_TABLE)

print("🏆 Top 10 seleções por overall médio:")
sf.select(
    "team_name","squad_avg_overall","squad_top11_overall",
    "squad_avg_age","squad_peak_pct","att_best_overall",
    "form_ppg","form_xg_diff","squad_value_m"
).orderBy("squad_avg_overall", ascending=False).show(10, truncate=False)

print("\n👴 Elencos mais velhos (risco de fadiga):")
sf.select("team_name","squad_avg_age","squad_veteran_pct","squad_top11_avg_age") \
  .orderBy("squad_avg_age", ascending=False).show(10, truncate=False)

print("\n📈 Melhor forma recente (PPG últimas 10):")
sf.select("team_name","form_ppg","form_xg_per_game","form_xga_per_game",
           "form_xg_diff","form_win_rate","form_clean_sheet_pct") \
  .orderBy("form_ppg", ascending=False).show(10, truncate=False)

print("\n💶 Elencos mais valiosos:")
sf.select("team_name","squad_value_m","avg_player_value_m",
           "top3_value_m","n_players_over_50m","depth_value_ratio") \
  .orderBy("squad_value_m", ascending=False).show(10, truncate=False)
