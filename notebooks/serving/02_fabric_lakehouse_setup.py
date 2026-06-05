# Databricks notebook source
# MAGIC %md
# MAGIC # Fabric — Setup do Lakehouse e Carga das Tabelas
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** Serving
# MAGIC **Executar:** No Microsoft Fabric Notebook (não no Databricks)
# MAGIC
# MAGIC Este notebook roda DENTRO do Fabric.
# MAGIC Lê os CSVs do OneDrive (via Files do Lakehouse) e cria
# MAGIC Delta tables no Lakehouse para o Power BI consumir.
# MAGIC
# MAGIC Pré-requisito: CSVs já estão em Files/wc-platform/ no Lakehouse.

# COMMAND ----------
# MAGIC %md ## Estrutura esperada no Lakehouse
# MAGIC
# MAGIC ```
# MAGIC Lakehouse: WC_DataPlatform
# MAGIC ├── Files/
# MAGIC │   └── wc-platform/
# MAGIC │       ├── top_scorers.csv
# MAGIC │       ├── top_scorers_cross_tournament.csv
# MAGIC │       ├── team_style.csv
# MAGIC │       ├── team_style_overall.csv
# MAGIC │       ├── match_features.csv
# MAGIC │       ├── standings.csv
# MAGIC │       ├── xg_predictions.csv
# MAGIC │       ├── match_predictions.csv
# MAGIC │       └── simulation_results.csv
# MAGIC └── Tables/
# MAGIC     └── (criadas por este notebook)
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 0. Configuração

# COMMAND ----------
LAKEHOUSE_NAME = "WC_DataPlatform"
FILES_PATH     = "Files/wc-platform"

# Mapeamento: arquivo CSV → nome da tabela Delta no Lakehouse
TABLE_MAP = {
    "top_scorers":                  "gold_top_scorers",
    "top_scorers_cross_tournament": "gold_top_scorers_cross",
    "team_style":                   "gold_team_style",
    "team_style_overall":           "gold_team_style_overall",
    "match_features":               "gold_match_features",
    "standings":                    "gold_standings",
    "xg_predictions":               "gold_xg_predictions",
    "match_predictions":            "gold_match_predictions",
    "simulation_results":           "gold_simulation_results",
}

# COMMAND ----------
# MAGIC %md ## 1. Verificar arquivos disponíveis

# COMMAND ----------
import os

base_path = f"/lakehouse/default/{FILES_PATH}"
files_found = []

for fname in os.listdir(base_path):
    if fname.endswith(".csv"):
        size = os.path.getsize(f"{base_path}/{fname}") / 1024
        files_found.append(fname)
        print(f"✅ {fname:45s} {size:.1f} KB")

missing = [f"{k}.csv" for k in TABLE_MAP if f"{k}.csv" not in files_found]
if missing:
    print(f"\n⚠️  Arquivos faltando: {missing}")
    print("   Fazer upload via: Lakehouse → Files → wc-platform → Upload")
else:
    print(f"\n✅ Todos os {len(TABLE_MAP)} arquivos encontrados!")

# COMMAND ----------
# MAGIC %md ## 2. Carregar CSVs e criar Delta Tables

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

loaded = []

for csv_name, table_name in TABLE_MAP.items():
    csv_path = f"Files/wc-platform/{csv_name}.csv"

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .option("nullValue", "")
            .option("multiLine", "false")
            .csv(csv_path)
        )

        n = df.count()

        # Adicionar metadados de carga
        df = df.withColumn("_fabric_loaded_at", F.current_timestamp()) \
               .withColumn("_source_file", F.lit(csv_name))

        # Escrever como Delta table no Lakehouse
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(table_name)

        loaded.append({"csv": csv_name, "table": table_name, "rows": n})
        print(f"✅ {table_name:35s} ← {csv_name}.csv ({n:,} linhas)")

    except Exception as e:
        print(f"❌ {csv_name}: {e}")

print(f"\n✅ {len(loaded)}/{len(TABLE_MAP)} tabelas carregadas no Lakehouse!")

# COMMAND ----------
# MAGIC %md ## 3. Validação rápida

# COMMAND ----------
print("Tabelas disponíveis no Lakehouse:")
spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'").show(20, truncate=False)

# Preview das mais importantes
for table in ["gold_simulation_results", "gold_top_scorers", "gold_standings"]:
    try:
        df = spark.table(table)
        print(f"\n--- {table} ({df.count()} linhas) ---")
        df.show(5, truncate=False)
    except Exception as e:
        print(f"⚠️  {table}: {e}")

# COMMAND ----------
# MAGIC %md ## 4. Criar view consolidada para o Power BI

# COMMAND ----------
# View de artilharia consolidada (mais útil para dashboards)
spark.sql("""
CREATE OR REPLACE VIEW vw_artilharia_geral AS
SELECT
    player_name,
    team_name,
    _competition_label  AS competition,
    goals,
    xg_total,
    ROUND(goals - xg_total, 2) AS goals_above_xg,
    assists,
    shots_total,
    shot_conversion_pct,
    goals_per_90,
    rank_in_competition
FROM gold_top_scorers
WHERE goals >= 1
ORDER BY goals DESC, xg_total DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_simulacao_copa2026 AS
SELECT
    team_name,
    elo_rating,
    `group`,
    p_round_of_16       AS pct_oitavas,
    p_quarter_finals    AS pct_quartas,
    p_semi_finals       AS pct_semi,
    p_final             AS pct_final,
    p_champion          AS pct_campeao,
    n_simulations
FROM gold_simulation_results
ORDER BY p_champion DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_estilos_taticos AS
SELECT
    team_name,
    _competition_label  AS competition,
    tactical_profile,
    avg_possession_pct,
    avg_ppda,
    xg_per_game,
    xga_per_game,
    xg_diff_per_game,
    dominance_score,
    matches_played,
    wins, draws, losses,
    win_pct
FROM gold_team_style
ORDER BY dominance_score DESC
""")

spark.sql("""
CREATE OR REPLACE VIEW vw_shot_map_xg AS
SELECT
    _competition_label  AS competition,
    player_name,
    team_name,
    shot_type,
    shot_technique,
    shot_body_part,
    distance_to_goal,
    angle_to_goal,
    our_xg,
    shot_xg             AS statsbomb_xg,
    CAST(is_goal AS INT) AS is_goal
FROM gold_xg_predictions
""")

print("✅ Views criadas:")
spark.sql("SHOW VIEWS").filter("viewName LIKE 'vw_%'").show(truncate=False)
