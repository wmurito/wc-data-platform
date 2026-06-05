# Databricks notebook source
# MAGIC %md
# MAGIC # Export — Gold Tables → CSV/Parquet para OneDrive
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** Serving
# MAGIC **Executar depois de:** Todas as fases Gold completas
# MAGIC
# MAGIC Exporta todas as tabelas Gold para CSV no DBFS.
# MAGIC Depois você faz download via Databricks UI e envia ao OneDrive.
# MAGIC Do OneDrive, o Fabric Lakehouse lê via atalho (shortcut).
# MAGIC
# MAGIC Fluxo completo:
# MAGIC   Databricks Gold (Delta) → DBFS CSV → Download local
# MAGIC   → Upload OneDrive → Fabric Lakehouse Shortcut → Power BI

# COMMAND ----------
# MAGIC %md ## 0. Configuração

# COMMAND ----------
CATALOG   = "worldcup"
SCHEMA_G  = "gold"
EXPORT_PATH = "dbfs:/FileStore/wc-platform/export"

# Todas as tabelas Gold para exportar
GOLD_TABLES = [
    "top_scorers",
    "top_scorers_cross_tournament",
    "team_style",
    "team_style_overall",
    "match_features",
    "standings",
    "xg_predictions",
    "match_predictions",
    "simulation_results",
]

try:
    spark.sql(f"USE CATALOG {CATALOG}")
    FULL_PREFIX = f"{CATALOG}.{SCHEMA_G}"
except Exception:
    FULL_PREFIX = f"hive_metastore.wc_gold"

# COMMAND ----------
# MAGIC %md ## 1. Exportar cada tabela como CSV single-file

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.fs.mkdirs(EXPORT_PATH)

exported = []

for table in GOLD_TABLES:
    full_name = f"{FULL_PREFIX}.{table}"
    out_path  = f"{EXPORT_PATH}/{table}"

    try:
        df = spark.table(full_name)
        n  = df.count()

        # Remover colunas internas de metadados que poluem o CSV
        drop_cols = [c for c in df.columns if c.startswith("_processed") or c == "_ingested_at"]
        df = df.drop(*drop_cols)

        # Coalescer para 1 arquivo (tabelas Gold são pequenas)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .option("sep", ",")
            .option("encoding", "UTF-8")
            .option("nullValue", "")
            .csv(out_path)
        )

        # Renomear o arquivo gerado (part-00000...) para nome legível
        files = [f for f in dbutils.fs.ls(out_path) if f.name.endswith(".csv")]
        if files:
            final_path = f"{EXPORT_PATH}/{table}.csv"
            dbutils.fs.cp(files[0].path, final_path)
            dbutils.fs.rm(out_path, recurse=True)
            size_kb = dbutils.fs.ls(final_path)[0].size / 1024
            exported.append({"table": table, "rows": n, "size_kb": round(size_kb, 1)})
            print(f"✅ {table}.csv — {n:,} linhas | {size_kb:.1f} KB")
        else:
            print(f"⚠️  {table}: arquivo CSV não encontrado após escrita")

    except Exception as e:
        print(f"❌ {table}: {e}")

# COMMAND ----------
# MAGIC %md ## 2. Resumo dos arquivos exportados

# COMMAND ----------
import pandas as pd

summary = pd.DataFrame(exported)
print("\n📦 Resumo da exportação:")
print(summary.to_string(index=False))
print(f"\nTotal: {summary['rows'].sum():,} linhas | {summary['size_kb'].sum():.1f} KB")
print(f"\nArquivos disponíveis em: {EXPORT_PATH}")

# COMMAND ----------
# MAGIC %md ## 3. Listar arquivos para download

# COMMAND ----------
print("\n📂 Arquivos no DBFS para download:")
for f in dbutils.fs.ls(EXPORT_PATH):
    if f.name.endswith(".csv"):
        size_kb = f.size / 1024
        print(f"  {f.name:45s} {size_kb:8.1f} KB")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Como fazer o download dos CSVs
# MAGIC
# MAGIC ### Opção A — Download individual via URL (mais simples)
# MAGIC Cada CSV fica disponível em:
# MAGIC ```
# MAGIC https://community.cloud.databricks.com/files/wc-platform/export/{table}.csv
# MAGIC ```
# MAGIC Substituir `community.cloud.databricks.com` pelo seu workspace.
# MAGIC
# MAGIC ### Opção B — Download via Databricks CLI
# MAGIC ```bash
# MAGIC databricks fs cp dbfs:/FileStore/wc-platform/export/ ./gold_exports/ --recursive
# MAGIC ```
# MAGIC
# MAGIC ### Opção C — Download via célula Python (gera link clicável)

# COMMAND ----------
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "community.cloud.databricks.com")

print("🔗 Links de download direto:")
for table in GOLD_TABLES:
    url = f"https://{workspace_url}/files/wc-platform/export/{table}.csv"
    print(f"  {table:40s}: {url}")
