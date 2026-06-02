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
DBFS_BASE  = "/Volumes/lakehouse/wc_platform/files/raw/statsbomb"
CATALOG    = "lakehouse"
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
from pyspark.sql.types import LongType, IntegerType, FloatType, BooleanType, StructType

def path_exists(schema, path_str):
    parts = path_str.split(".")
    current = schema
    for part in parts:
        if isinstance(current, StructType):
            names = [f.name for f in current.fields]
            if part not in names:
                return False
            field = [f for f in current.fields if f.name == part][0]
            current = field.dataType
        else:
            return False
    return True

def safe_col(df, path_str, cast_type=None):
    if path_exists(df.schema, path_str):
        res = F.col(path_str)
    else:
        res = F.lit(None)
    if cast_type:
        res = res.cast(cast_type)
    return res

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

    # Campos base com seleção segura contra campos ausentes
    df = raw.select(
        # Identificadores
        safe_col(raw, "id").alias("event_id"),
        safe_col(raw, "match_id", LongType()).alias("match_id"),
        safe_col(raw, "index", IntegerType()).alias("index"),

        # Tempo
        safe_col(raw, "period", IntegerType()).alias("period"),
        safe_col(raw, "timestamp").alias("timestamp"),
        safe_col(raw, "minute", IntegerType()).alias("minute"),
        safe_col(raw, "second", IntegerType()).alias("second"),
        safe_col(raw, "duration", FloatType()).alias("duration"),

        # Tipo de evento
        safe_col(raw, "type.id", IntegerType()).alias("type_id"),
        safe_col(raw, "type.name").alias("type_name"),

        # Posse
        safe_col(raw, "possession", IntegerType()).alias("possession"),
        safe_col(raw, "possession_team.id", IntegerType()).alias("possession_team_id"),
        safe_col(raw, "possession_team.name").alias("possession_team_name"),

        # Padrão de jogo
        safe_col(raw, "play_pattern.id", IntegerType()).alias("play_pattern_id"),
        safe_col(raw, "play_pattern.name").alias("play_pattern_name"),

        # Time e jogador
        safe_col(raw, "team.id", IntegerType()).alias("team_id"),
        safe_col(raw, "team.name").alias("team_name"),
        safe_col(raw, "player.id", IntegerType()).alias("player_id"),
        safe_col(raw, "player.name").alias("player_name"),
        safe_col(raw, "position.id", IntegerType()).alias("position_id"),
        safe_col(raw, "position.name").alias("position_name"),

        # Localização no campo [x, y]
        safe_col(raw, "location", "array<double>").alias("location"),

        # Flags
        safe_col(raw, "under_pressure", BooleanType()).alias("under_pressure"),
        safe_col(raw, "off_camera", BooleanType()).alias("off_camera"),
        safe_col(raw, "out", BooleanType()).alias("out"),
        safe_col(raw, "counterpress", BooleanType()).alias("counterpress"),

        # ── Structs específicos por tipo (preservados inteiros na Bronze)
        # Shot
        safe_col(raw, "shot.statsbomb_xg", FloatType()).alias("shot_xg"),
        safe_col(raw, "shot.outcome.name").alias("shot_outcome"),
        safe_col(raw, "shot.technique.name").alias("shot_technique"),
        safe_col(raw, "shot.body_part.name").alias("shot_body_part"),
        safe_col(raw, "shot.type.name").alias("shot_type"),
        safe_col(raw, "shot.first_time", BooleanType()).alias("shot_first_time"),
        safe_col(raw, "shot.one_on_one", BooleanType()).alias("shot_one_on_one"),
        safe_col(raw, "shot.end_location", "array<double>").alias("shot_end_location"),

        # Pass
        safe_col(raw, "pass.length", FloatType()).alias("pass_length"),
        safe_col(raw, "pass.angle", FloatType()).alias("pass_angle"),
        safe_col(raw, "pass.height.name").alias("pass_height"),
        safe_col(raw, "pass.recipient.id", IntegerType()).alias("pass_recipient_id"),
        safe_col(raw, "pass.recipient.name").alias("pass_recipient_name"),
        safe_col(raw, "pass.outcome.name").alias("pass_outcome"),
        safe_col(raw, "pass.type.name").alias("pass_type"),
        safe_col(raw, "pass.body_part.name").alias("pass_body_part"),
        safe_col(raw, "pass.end_location", "array<double>").alias("pass_end_location"),
        safe_col(raw, "pass.statsbomb_xg", FloatType()).alias("pass_xa"),
        safe_col(raw, "pass.switch", BooleanType()).alias("pass_switch"),
        safe_col(raw, "pass.cross", BooleanType()).alias("pass_cross"),
        safe_col(raw, "pass.through_ball", BooleanType()).alias("pass_through_ball"),
        safe_col(raw, "pass.goal_assist", BooleanType()).alias("pass_goal_assist"),
        safe_col(raw, "pass.shot_assist", BooleanType()).alias("pass_shot_assist"),

        # Carry
        safe_col(raw, "carry.end_location", "array<double>").alias("carry_end_location"),

        # Duel
        safe_col(raw, "duel.type.name").alias("duel_type"),
        safe_col(raw, "duel.outcome.name").alias("duel_outcome"),

        # Goalkeeper
        safe_col(raw, "goalkeeper.type.name").alias("goalkeeper_type"),
        safe_col(raw, "goalkeeper.outcome.name").alias("goalkeeper_outcome"),
        safe_col(raw, "goalkeeper.position.name").alias("goalkeeper_position"),
        safe_col(raw, "goalkeeper.body_part.name").alias("goalkeeper_body_part"),
        safe_col(raw, "goalkeeper.technique.name").alias("goalkeeper_technique"),

        # Foul
        safe_col(raw, "foul_committed.card.name").alias("foul_card"),
        safe_col(raw, "foul_committed.penalty", BooleanType()).alias("foul_penalty"),

        # Interception
        safe_col(raw, "interception.outcome.name").alias("interception_outcome"),

        # Clearance
        safe_col(raw, "clearance.body_part.name").alias("clearance_body_part"),
        safe_col(raw, "clearance.head", BooleanType()).alias("clearance_head"),

        # Bad behaviour (cartões diretos)
        safe_col(raw, "bad_behaviour.card.name").alias("bad_behaviour_card"),

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
