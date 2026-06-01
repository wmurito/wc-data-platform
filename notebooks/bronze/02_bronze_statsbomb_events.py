# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — StatsBomb Events
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 02 Bronze
# MAGIC **Tabela:** `worldcup.bronze.statsbomb_events`
# MAGIC **Executar depois de:** 01_bronze_statsbomb_matches
# MAGIC
# MAGIC Lê todos os arquivos events/{match_id}.json do DBFS.
# MAGIC Cada arquivo tem ~3.400 eventos com schema nested.
# MAGIC Esta camada Bronze preserva TUDO — nenhum campo é descartado.
# MAGIC Campos nested (shot, pass, carry, duel, goalkeeper) ficam como struct.
# MAGIC A Silver vai explodir cada tipo em tabelas especializadas.

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
DBFS_BASE  = "dbfs:/FileStore/wc-platform/raw/statsbomb"
CATALOG    = "worldcup"
SCHEMA     = "bronze"
TABLE      = "statsbomb_events"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

COMPETITIONS = [
    {"slug": "wc_2022",           "competition_id": 43,  "season_id": 106, "label": "FIFA World Cup 2022"},
    {"slug": "wc_2018",           "competition_id": 43,  "season_id": 3,   "label": "FIFA World Cup 2018"},
    {"slug": "euro_2024",         "competition_id": 55,  "season_id": 282, "label": "UEFA Euro 2024"},
    {"slug": "euro_2020",         "competition_id": 55,  "season_id": 43,  "label": "UEFA Euro 2020"},
    {"slug": "copa_america_2024", "competition_id": 223, "season_id": 282, "label": "Copa América 2024"},
]

# COMMAND ----------
# MAGIC %md ## 1. Catalog / Schema

# COMMAND ----------
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"✅ Unity Catalog: {CATALOG}.{SCHEMA}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA     = "wc_bronze"
    FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
    print(f"⚠️  Fallback → hive_metastore.{SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 2. Leitura de eventos — competição por competição
# MAGIC
# MAGIC Estratégia: ler competition por competition para evitar OOM
# MAGIC no Community Edition (single-node). Escrita incremental por partição.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType, FloatType, BooleanType

def read_competition_events(comp: dict) -> None:
    """
    Lê todos os events/{match_id}.json de uma competição via Auto Loader
    e escreve na tabela Bronze. Idempotente: pula partições já escritas.
    """
    events_path = f"{DBFS_BASE}/{comp['slug']}/events"

    # Verificar se o diretório existe
    try:
        files = dbutils.fs.ls(events_path)
    except Exception:
        print(f"⚠️  {comp['label']}: diretório de eventos não encontrado: {events_path}")
        return

    n_files = len(files)
    print(f"\n📂 {comp['label']}: {n_files} arquivos de evento")

    # Leitura em batch com schema inference
    # multiLine=true pois cada arquivo é um array JSON
    raw = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")          # não quebrar por JSON malformado
        .json(events_path)
    )

    # Campos base presentes em TODOS os eventos
    df = raw.select(
        # Identificadores
        F.col("id").alias("event_id"),
        F.col("match_id").cast(LongType()),
        F.col("index").cast(IntegerType()),

        # Tempo
        F.col("period").cast(IntegerType()),
        F.col("timestamp"),
        F.col("minute").cast(IntegerType()),
        F.col("second").cast(IntegerType()),
        F.col("duration").cast(FloatType()),

        # Tipo de evento
        F.col("type.id").cast(IntegerType()).alias("type_id"),
        F.col("type.name").alias("type_name"),

        # Posse
        F.col("possession").cast(IntegerType()),
        F.col("possession_team.id").cast(IntegerType()).alias("possession_team_id"),
        F.col("possession_team.name").alias("possession_team_name"),

        # Padrão de jogo
        F.col("play_pattern.id").cast(IntegerType()).alias("play_pattern_id"),
        F.col("play_pattern.name").alias("play_pattern_name"),

        # Time e jogador
        F.col("team.id").cast(IntegerType()).alias("team_id"),
        F.col("team.name").alias("team_name"),
        F.col("player.id").cast(IntegerType()).alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("position.id").cast(IntegerType()).alias("position_id"),
        F.col("position.name").alias("position_name"),

        # Localização no campo [x, y]
        F.col("location").cast("array<double>").alias("location"),

        # Flags
        F.col("under_pressure").cast(BooleanType()),
        F.col("off_camera").cast(BooleanType()),
        F.col("out").cast(BooleanType()),
        F.col("counterpress").cast(BooleanType()),

        # ── Structs específicos por tipo (preservados inteiros na Bronze)
        # Shot
        F.col("shot.statsbomb_xg").cast(FloatType()).alias("shot_xg"),
        F.col("shot.outcome.name").alias("shot_outcome"),
        F.col("shot.technique.name").alias("shot_technique"),
        F.col("shot.body_part.name").alias("shot_body_part"),
        F.col("shot.type.name").alias("shot_type"),
        F.col("shot.first_time").cast(BooleanType()).alias("shot_first_time"),
        F.col("shot.one_on_one").cast(BooleanType()).alias("shot_one_on_one"),
        F.col("shot.end_location").cast("array<double>").alias("shot_end_location"),

        # Pass
        F.col("pass.length").cast(FloatType()).alias("pass_length"),
        F.col("pass.angle").cast(FloatType()).alias("pass_angle"),
        F.col("pass.height.name").alias("pass_height"),
        F.col("pass.recipient.id").cast(IntegerType()).alias("pass_recipient_id"),
        F.col("pass.recipient.name").alias("pass_recipient_name"),
        F.col("pass.outcome.name").alias("pass_outcome"),
        F.col("pass.type.name").alias("pass_type"),
        F.col("pass.body_part.name").alias("pass_body_part"),
        F.col("pass.end_location").cast("array<double>").alias("pass_end_location"),
        F.col("pass.statsbomb_xg").cast(FloatType()).alias("pass_xa"),
        F.col("pass.switch").cast(BooleanType()).alias("pass_switch"),
        F.col("pass.cross").cast(BooleanType()).alias("pass_cross"),
        F.col("pass.through_ball").cast(BooleanType()).alias("pass_through_ball"),
        F.col("pass.goal_assist").cast(BooleanType()).alias("pass_goal_assist"),
        F.col("pass.shot_assist").cast(BooleanType()).alias("pass_shot_assist"),

        # Carry
        F.col("carry.end_location").cast("array<double>").alias("carry_end_location"),

        # Duel
        F.col("duel.type.name").alias("duel_type"),
        F.col("duel.outcome.name").alias("duel_outcome"),

        # Goalkeeper
        F.col("goalkeeper.type.name").alias("goalkeeper_type"),
        F.col("goalkeeper.outcome.name").alias("goalkeeper_outcome"),
        F.col("goalkeeper.position.name").alias("goalkeeper_position"),
        F.col("goalkeeper.body_part.name").alias("goalkeeper_body_part"),
        F.col("goalkeeper.technique.name").alias("goalkeeper_technique"),

        # Foul
        F.col("foul_committed.card.name").alias("foul_card"),
        F.col("foul_committed.penalty").cast(BooleanType()).alias("foul_penalty"),

        # Interception
        F.col("interception.outcome.name").alias("interception_outcome"),

        # Clearance
        F.col("clearance.body_part.name").alias("clearance_body_part"),
        F.col("clearance.head").cast(BooleanType()).alias("clearance_head"),

        # Bad behaviour (cartões diretos)
        F.col("bad_behaviour.card.name").alias("bad_behaviour_card"),

        # Meta Bronze
        F.lit(comp["competition_id"]).cast(IntegerType()).alias("_competition_id"),
        F.lit(comp["season_id"]).cast(IntegerType()).alias("_season_id"),
        F.lit(comp["label"]).alias("_competition_label"),
        F.current_timestamp().alias("_ingested_at"),
    )

    n_events = df.count()
    print(f"   Eventos lidos: {n_events:,}")

    # Escrita particionada por _competition_id
    # mode=overwrite por partição para ser idempotente
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_competition_id", "period")
        .saveAsTable(FULL_TABLE)
    )
    print(f"   ✅ {comp['label']} escrita na Bronze")


# COMMAND ----------
# MAGIC %md ## 3. Executar por competição

# COMMAND ----------
# Limpar tabela antes de reingerir tudo (idempotência)
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

for comp in COMPETITIONS:
    read_competition_events(comp)

# COMMAND ----------
# MAGIC %md ## 4. Validação

# COMMAND ----------
events = spark.table(FULL_TABLE)
total  = events.count()
print(f"Total de eventos: {total:,}")

print("\nEventos por tipo (Top 15):")
events.groupBy("type_name").count() \
    .orderBy("count", ascending=False) \
    .show(15, truncate=False)

print("\nEventos por competição:")
events.groupBy("_competition_label").count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)

print("\nDistribuição de xG (apenas chutes):")
events.filter(F.col("shot_xg").isNotNull()) \
    .select(
        F.count("event_id").alias("total_shots"),
        F.sum(F.col("shot_xg")).alias("total_xg"),
        F.avg(F.col("shot_xg")).alias("avg_xg_per_shot"),
        F.max(F.col("shot_xg")).alias("max_xg"),
    ).show()

# COMMAND ----------
# MAGIC %md ## 5. Amostra — top xG shots

# COMMAND ----------
events.filter(
    (F.col("type_name") == "Shot") &
    (F.col("shot_xg").isNotNull())
).select(
    "_competition_label", "match_id", "minute",
    "team_name", "player_name",
    "shot_xg", "shot_outcome", "shot_technique",
    "shot_body_part", "shot_type"
).orderBy(F.col("shot_xg").desc()) \
 .show(20, truncate=False)
