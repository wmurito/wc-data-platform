# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — FBref Player Stats (Temporada 2024-25)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.fbref_player_season`
# MAGIC **Fonte:** FBref via soccerdata — top-5 ligas europeias
# MAGIC **Executar depois de:** script `ingestion/02_fbref_scraper.py` + upload para DBFS
# MAGIC
# MAGIC Consolida os 5 arquivos Parquet por categoria de stat
# MAGIC (shooting, passing, defense, possession, misc) em uma
# MAGIC única tabela Bronze normalizada.
# MAGIC
# MAGIC Por que isso importa:
# MAGIC   xG do jogador no clube → feature para predição de gols na Copa 2026.
# MAGIC   Vinícius Jr. com xG=0.62/90 no Real Madrid → modelo usa isso.

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
DBFS_FBREF = "/Volumes/lakehouse/wc_platform/files/raw/fbref"
CATALOG    = "lakehouse"
SCHEMA     = "bronze"
TABLE      = "fbref_player_season"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
SEASON     = "2024-25"

STAT_FILES = {
    "shooting":   f"{DBFS_FBREF}/shooting_{SEASON}.parquet",
    "passing":    f"{DBFS_FBREF}/passing_{SEASON}.parquet",
    "defense":    f"{DBFS_FBREF}/defense_{SEASON}.parquet",
    "possession": f"{DBFS_FBREF}/possession_{SEASON}.parquet",
    "misc":       f"{DBFS_FBREF}/misc_{SEASON}.parquet",
}

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
# MAGIC %md ## 2. Leitura dos arquivos Parquet por categoria

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

dfs = {}
for stat, path in STAT_FILES.items():
    try:
        df = spark.read.parquet(path)
        dfs[stat] = df
        print(f"✅ {stat}: {df.count():,} registros | {len(df.columns)} colunas")
        # Mostrar colunas disponíveis
        print(f"   Colunas: {df.columns[:10]}{'...' if len(df.columns)>10 else ''}\n")
    except Exception as e:
        print(f"⚠️  {stat}: não encontrado — {e}")

# COMMAND ----------
# MAGIC %md ## 3. Normalização — shooting (mais importante para o projeto)
# MAGIC
# MAGIC FBref usa MultiIndex de colunas — soccerdata já aplana para `categoria_coluna`.
# MAGIC Mapeamos os nomes reais para aliases legíveis.

# COMMAND ----------
def normalize_shooting(df):
    """
    Normaliza o DataFrame de shooting do FBref.
    Colunas reais do soccerdata (aplainadas do MultiIndex):
      player, nation, pos, squad, comp, age, born, Playing Time_90s,
      Performance_Gls, Performance_Sh, Performance_SoT, Performance_SoT%,
      Performance_Sh/90, Performance_SoT/90, Performance_G/Sh, Performance_G/SoT,
      Expected_xG, Expected_npxG, Expected_xG/Sh, Expected_G-xG, Expected_np:G-xG
    """
    available = [c.lower() for c in df.columns]

    # Mapeamento flexível — tenta variações de nomes
    def get_col(candidates, alias, cast_type=None):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches:
                col_expr = F.col(matches[0])
                if cast_type:
                    col_expr = col_expr.cast(cast_type)
                return col_expr.alias(alias)
        return F.lit(None).cast(cast_type or "string").alias(alias)

    return df.select(
        get_col(["player"],         "player_name"),
        get_col(["squad"],          "club"),
        get_col(["comp"],           "league"),
        get_col(["nation"],         "nationality"),
        get_col(["pos"],            "position"),
        get_col(["age"],            "age", IntegerType()),
        get_col(["90s","_90s"],     "nineties_played", FloatType()),
        # Gols e chutes
        get_col(["gls","_gls","performance_gls"], "goals", IntegerType()),
        get_col(["sh","_sh","performance_sh"],     "shots", IntegerType()),
        get_col(["sot","_sot","performance_sot"],  "shots_on_target", IntegerType()),
        get_col(["sot%","sot_pct"],                "shot_accuracy_pct", FloatType()),
        get_col(["sh/90","sh_90"],                 "shots_per_90", FloatType()),
        # xG
        get_col(["xg","expected_xg"],              "xg", FloatType()),
        get_col(["npxg","expected_npxg"],          "npxg", FloatType()),
        get_col(["xg/sh"],                         "xg_per_shot", FloatType()),
        get_col(["g-xg","g_xg"],                   "goals_minus_xg", FloatType()),
        # Meta
        F.lit("shooting").alias("stat_category"),
        F.lit(SEASON).alias("season"),
        F.current_timestamp().alias("_ingested_at"),
    )


def normalize_passing(df):
    return df.select(
        F.col([c for c in df.columns if "player" in c.lower()][0]).alias("player_name"),
        F.col([c for c in df.columns if "squad" in c.lower()][0]).alias("club"),
        F.col([c for c in df.columns if "comp" in c.lower()][0]).alias("league"),
        *[
            F.col(c).cast(FloatType()).alias(
                c.lower()
                 .replace(" ","_").replace("/","_per_").replace("%","_pct")
                 .replace("-","_").replace(":","_")[:60]
            )
            for c in df.columns
            if any(kw in c.lower() for kw in ["xag","xa","kp","prgp","cmp%","totdist"])
        ],
        F.lit("passing").alias("stat_category"),
        F.lit(SEASON).alias("season"),
        F.current_timestamp().alias("_ingested_at"),
    )


def normalize_defense(df):
    return df.select(
        F.col([c for c in df.columns if "player" in c.lower()][0]).alias("player_name"),
        F.col([c for c in df.columns if "squad" in c.lower()][0]).alias("club"),
        F.col([c for c in df.columns if "comp" in c.lower()][0]).alias("league"),
        *[
            F.col(c).cast(FloatType()).alias(
                c.lower().replace(" ","_").replace("/","_per_").replace("%","_pct")[:60]
            )
            for c in df.columns
            if any(kw in c.lower() for kw in ["tkl","press","blocks","int","clr"])
        ],
        F.lit("defense").alias("stat_category"),
        F.lit(SEASON).alias("season"),
        F.current_timestamp().alias("_ingested_at"),
    )


# COMMAND ----------
# MAGIC %md ## 4. Processamento e escrita

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

stat_processors = {
    "shooting":   normalize_shooting,
    "passing":    normalize_passing,
    "defense":    normalize_defense,
}

for stat, processor in stat_processors.items():
    if stat not in dfs:
        print(f"⚠️  {stat}: sem dados, pulando")
        continue
    try:
        normalized = processor(dfs[stat])
        normalized.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("stat_category") \
            .saveAsTable(FULL_TABLE)
        print(f"✅ {stat}: {normalized.count():,} registros escritos")
    except Exception as e:
        print(f"⚠️  {stat}: erro na normalização — {e}")

# COMMAND ----------
# MAGIC %md ## 5. Validação

# COMMAND ----------
fbref = spark.table(FULL_TABLE)
print(f"Total de registros: {fbref.count():,}")

print("\nRegistros por categoria e liga:")
fbref.groupBy("stat_category","league") \
    .count() \
    .orderBy("stat_category","count", ascending=False) \
    .show(30, truncate=False)

# COMMAND ----------
# MAGIC %md ## 6. Top xG por jogador — shooting

# COMMAND ----------
fbref.filter(
    (F.col("stat_category") == "shooting") &
    (F.col("xg").isNotNull()) &
    (F.col("xg") > 5)
).select(
    "player_name","club","league","position",
    "goals","shots","xg","npxg","xg_per_shot","goals_minus_xg"
).orderBy(F.col("xg").desc()) \
 .show(25, truncate=False)
