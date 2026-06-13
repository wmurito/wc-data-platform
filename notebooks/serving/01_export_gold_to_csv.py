# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Export — Gold Tables → Parquet para OneDrive/Fabric
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** Serving
# MAGIC **Executar depois de:** Todas as fases Gold completas
# MAGIC
# MAGIC Exporta todas as tabelas Gold para Parquet em Unity Catalog Volume.
# MAGIC Parquet suporta tipos complexos (arrays, structs) e é muito mais eficiente que CSV.
# MAGIC
# MAGIC Fluxo completo:
# MAGIC   Databricks Gold (Delta) → Volume UC Parquet → Download local
# MAGIC   → Upload OneDrive → Fabric Lakehouse Shortcut → Power BI

# COMMAND ----------

# MAGIC %md ## 0. Configuração

# COMMAND ----------

CATALOG   = "lakehouse"
SCHEMA_G  = "gold"

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

# DBTITLE 1,Cell 5
from pyspark.sql import functions as F

# Criar Volume UC se não existir (serverless não permite DBFS)
spark.sql("CREATE VOLUME IF NOT EXISTS lakehouse.gold.wc_export")
EXPORT_PATH = "/Volumes/lakehouse/gold/wc_export"

exported = []

for table in GOLD_TABLES:
    full_name = f"{FULL_PREFIX}.{table}"

    try:
        df = spark.table(full_name)
        n  = df.count()

        # Remover colunas internas de metadados
        drop_cols = [c for c in df.columns if c.startswith("_processed") or c == "_ingested_at"]
        df = df.drop(*drop_cols)

        # Exportar para Parquet (suporta tipos complexos como arrays)
        final_path = f"{EXPORT_PATH}/{table}.parquet"
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(final_path)
        )

        # Volume write cria part-* files, mover o Parquet para nome final
        files = [f for f in dbutils.fs.ls(final_path) if f.name.endswith(".parquet")]
        if files:
            temp_parquet = files[0].path
            renamed_path = f"{EXPORT_PATH}/{table}_final.parquet"
            dbutils.fs.mv(temp_parquet, renamed_path)
            # Limpar arquivos auxiliares (_SUCCESS, _committed, etc)
            for f in dbutils.fs.ls(final_path):
                if not f.name.endswith("_final.parquet"):
                    dbutils.fs.rm(f.path, recurse=True)
            size_kb = dbutils.fs.ls(renamed_path)[0].size / 1024
            exported.append({"table": table, "rows": n, "size_kb": round(size_kb, 1)})
            print(f"✅ {table}.parquet — {n:,} linhas | {size_kb:.1f} KB")
        else:
            print(f"⚠️  {table}: arquivo Parquet não encontrado após escrita")

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

# DBTITLE 1,Cell 9
print("\n📂 Arquivos exportados no Volume UC:")
for f in dbutils.fs.ls(EXPORT_PATH):
    if f.name.endswith(".parquet"):
        size_kb = f.size / 1024
        print(f"  {f.name:45s} {size_kb:8.1f} KB")

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %md
# MAGIC ## 4. Como fazer o download dos arquivos Parquet
# MAGIC
# MAGIC ### Opção A — Download via Databricks UI (mais simples)
# MAGIC 1. Ir em **Data** → **Volumes**
# MAGIC 2. Navegar: `lakehouse` → `gold` → `wc_export`
# MAGIC 3. Clicar no arquivo e fazer download
# MAGIC
# MAGIC ### Opção B — Download via Databricks CLI
# MAGIC ```bash
# MAGIC databricks fs cp /Volumes/lakehouse/gold/wc_export/ ./gold_exports/ --recursive
# MAGIC ```
# MAGIC
# MAGIC ### Opção C — Ler direto no Fabric via Delta Sharing
# MAGIC Mais eficiente que download/upload: criar share UC e conectar no Fabric.

# COMMAND ----------

# DBTITLE 1,Cell 11
# Para Volume UC, o acesso é via workspace file browser ou dbutils
print("🔗 Caminho dos arquivos Parquet:")
for table in GOLD_TABLES:
    path = f"{EXPORT_PATH}/{table}_final.parquet"
    print(f"  {table:40s}: {path}")

print("\n📄 Para acessar: Data > Volumes > lakehouse > gold > wc_export")

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %md
# MAGIC ## 5. Delta Sharing — Acesso direto no Fabric/Power BI (Recomendado)
# MAGIC
# MAGIC **Vantagens:**
# MAGIC * ✅ Sem download/upload manual de arquivos
# MAGIC * ✅ Dados sempre atualizados em tempo real
# MAGIC * ✅ Controle de acesso centralizado no Databricks
# MAGIC * ✅ Performance superior (leitura incremental)
# MAGIC * ✅ Suporta todos os tipos de dados (arrays, structs, etc)
# MAGIC
# MAGIC **Arquitetura:**
# MAGIC ```
# MAGIC Databricks UC Tables (Delta) 
# MAGIC     ↓ Delta Sharing Protocol
# MAGIC Microsoft Fabric Lakehouse
# MAGIC     ↓ DirectLake/Import
# MAGIC Power BI
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %sql
# MAGIC -- Passo 1: Criar o Share (conjunto de tabelas compartilhadas)
# MAGIC CREATE SHARE IF NOT EXISTS wc_gold_share;
# MAGIC
# MAGIC -- Verificar share criado
# MAGIC SHOW SHARES;

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %sql
# MAGIC -- Passo 2: Adicionar todas as tabelas Gold ao Share
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.top_scorers;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.top_scorers_cross_tournament;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.team_style;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.team_style_overall;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.match_features;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.standings;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.xg_predictions;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.match_predictions;
# MAGIC ALTER SHARE wc_gold_share ADD TABLE lakehouse.gold.simulation_results;
# MAGIC
# MAGIC -- Verificar tabelas no share
# MAGIC SHOW ALL IN SHARE wc_gold_share;

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %sql
# MAGIC -- Passo 3: Criar Recipient (destinatário que vai acessar)
# MAGIC -- Token-based: gera credenciais para acesso externo
# MAGIC CREATE RECIPIENT IF NOT EXISTS fabric_powerbi;
# MAGIC
# MAGIC -- Verificar recipients
# MAGIC SHOW RECIPIENTS;

# COMMAND ----------

# DBTITLE 1,⚠️ Requisitos Delta Sharing
# MAGIC %md
# MAGIC ### ⚠️ Requisitos para Delta Sharing
# MAGIC
# MAGIC **Erro encontrado:** `External Delta Sharing is not enabled on the metastore`
# MAGIC
# MAGIC Para usar Delta Sharing externo, você precisa:
# MAGIC
# MAGIC 1. **Habilitar Delta Sharing no metastore** (requer permissões de administrador):
# MAGIC    * Ir em **Data** → **Delta Sharing** → **Manage** 
# MAGIC    * Ativar **"Enable Delta Sharing"**
# MAGIC    * Ou via API: `ALTER METASTORE SET DELTA_SHARING = ENABLED`
# MAGIC
# MAGIC 2. **Alternativa 1: Usar exportação Parquet** (já implementado ✅)
# MAGIC    * Células 5-9 exportam para `/Volumes/lakehouse/gold/wc_export`
# MAGIC    * Fazer download e upload para OneDrive/Fabric manualmente
# MAGIC    * Funciona sem permissões especiais
# MAGIC
# MAGIC 3. **Alternativa 2: Databricks-to-Databricks Sharing**
# MAGIC    * Se o destino também for Databricks, use compartilhamento direto
# MAGIC    * Não requer Delta Sharing externo
# MAGIC    * `GRANT SELECT ON TABLE lakehouse.gold.* TO USER <email>`
# MAGIC
# MAGIC 4. **Alternativa 3: Data Lakehouse Federation (Power BI)**
# MAGIC    * Conectar Power BI diretamente às tabelas UC via SQL Warehouse
# MAGIC    * Usar conector Databricks SQL no Power BI
# MAGIC    * Leitura direta sem exportação
# MAGIC
# MAGIC ### Próximos passos:
# MAGIC * Contatar o administrador do workspace para habilitar Delta Sharing, OU
# MAGIC * Usar a exportação Parquet (Células 5-9) para download manual

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC -- Passo 4: Conceder acesso ao Recipient
# MAGIC GRANT SELECT ON SHARE wc_gold_share TO RECIPIENT fabric_powerbi;
# MAGIC
# MAGIC -- Verificar permissões
# MAGIC SHOW GRANTS ON SHARE wc_gold_share;

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC -- Passo 5: Obter as credenciais de conexão
# MAGIC DESCRIBE RECIPIENT fabric_powerbi;

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %md
# MAGIC ## 6. Configurar no Microsoft Fabric
# MAGIC
# MAGIC ### Passo A: Copiar credenciais
# MAGIC Na célula anterior (`DESCRIBE RECIPIENT`), você receberá:
# MAGIC * **activation_link**: URL para ativar a conexão
# MAGIC * **share_credentials_file**: Arquivo JSON com credenciais
# MAGIC
# MAGIC ### Passo B: Conectar no Fabric Lakehouse
# MAGIC 1. Abrir **Microsoft Fabric** → **Lakehouse**
# MAGIC 2. Criar novo **Shortcut**
# MAGIC 3. Escolher **Delta Sharing** como fonte
# MAGIC 4. Colar:
# MAGIC    * **Endpoint URL**: da resposta do `DESCRIBE RECIPIENT`
# MAGIC    * **Bearer Token** ou **Credential File**: do JSON retornado
# MAGIC 5. Selecionar as tabelas desejadas
# MAGIC
# MAGIC ### Passo C: Usar no Power BI
# MAGIC 1. Abrir **Power BI Desktop**
# MAGIC 2. **Obter Dados** → **Mais** → **Microsoft Fabric**
# MAGIC 3. Conectar ao Lakehouse criado no passo anterior
# MAGIC 4. Selecionar as tabelas Gold
# MAGIC 5. Escolher modo **DirectLake** (recomendado) ou **Import**
# MAGIC
# MAGIC ### Alternativa: Conectar direto no Power BI
# MAGIC 1. **Obter Dados** → **Databricks**
# MAGIC 2. Usar credenciais do Delta Share
# MAGIC 3. Autenticar e selecionar tabelas

# COMMAND ----------

# DBTITLE 1,Cell 19
# MAGIC %md
# MAGIC ## 7. Comandos úteis de gerenciamento
# MAGIC
# MAGIC ```sql
# MAGIC -- Ver todos os shares
# MAGIC SHOW SHARES;
# MAGIC
# MAGIC -- Ver detalhes de um share específico
# MAGIC DESCRIBE SHARE wc_gold_share;
# MAGIC
# MAGIC -- Ver tabelas compartilhadas
# MAGIC SHOW ALL IN SHARE wc_gold_share;
# MAGIC
# MAGIC -- Ver recipients
# MAGIC SHOW RECIPIENTS;
# MAGIC
# MAGIC -- Ver permissões
# MAGIC SHOW GRANTS ON SHARE wc_gold_share;
# MAGIC
# MAGIC -- Remover tabela do share (se necessário)
# MAGIC -- ALTER SHARE wc_gold_share REMOVE TABLE lakehouse.gold.standings;
# MAGIC
# MAGIC -- Revogar acesso
# MAGIC -- REVOKE SELECT ON SHARE wc_gold_share FROM RECIPIENT fabric_powerbi_open;
# MAGIC
# MAGIC -- Excluir share (cuidado!)
# MAGIC -- DROP SHARE IF EXISTS wc_gold_share;
# MAGIC
# MAGIC -- Excluir recipient
# MAGIC -- DROP RECIPIENT IF EXISTS fabric_powerbi_open;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Conexão direta Power BI
# MAGIC %md
# MAGIC ## 8. Conectar Power BI diretamente (sem exportação)
# MAGIC
# MAGIC ### Método recomendado sem Delta Sharing: SQL Warehouse
# MAGIC
# MAGIC **Vantagens:**
# MAGIC * ✅ Sem download/upload de arquivos
# MAGIC * ✅ Dados sempre atualizados
# MAGIC * ✅ Funciona sem Delta Sharing habilitado
# MAGIC * ✅ Performance otimizada (Photon, cache)
# MAGIC
# MAGIC ### Passos:
# MAGIC
# MAGIC #### 1. Obter credenciais do SQL Warehouse
# MAGIC 1. No Databricks: **SQL** → **SQL Warehouses**
# MAGIC 2. Selecionar um warehouse (ou criar)
# MAGIC 3. Ir em **Connection details**
# MAGIC 4. Copiar:
# MAGIC    * **Server hostname**: `xxxxx.cloud.databricks.com`
# MAGIC    * **HTTP path**: `/sql/1.0/warehouses/xxxxx`
# MAGIC    * **Access token**: Gerar em **User Settings** → **Access tokens**
# MAGIC
# MAGIC #### 2. Conectar no Power BI Desktop
# MAGIC 1. **Obter Dados** → **Mais** → **Databricks**
# MAGIC 2. Colar **Server hostname** e **HTTP path**
# MAGIC 3. Modo de conectividade: **DirectQuery** (recomendado) ou **Import**
# MAGIC 4. Autenticar com **Personal Access Token**
# MAGIC 5. Selecionar:
# MAGIC    * Catalog: `lakehouse`
# MAGIC    * Schema: `gold`
# MAGIC    * Tabelas: todas as 9 tabelas Gold
# MAGIC
# MAGIC #### 3. Publicar no Power BI Service
# MAGIC 1. Salvar e publicar o relatório
# MAGIC 2. Configurar refresh agendado (se Import)
# MAGIC 3. Para DirectQuery: dados sempre atualizados automaticamente
