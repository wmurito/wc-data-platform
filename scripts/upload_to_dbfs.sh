#!/bin/bash
# WorldCup Data Platform — Upload de Seeds para DBFS
# Requer: Databricks CLI configurado (databricks configure --token)
# Docs: https://docs.databricks.com/dev-tools/cli/index.html

set -e

DBFS_PATH="dbfs:/FileStore/wc-platform/seeds"
LOCAL_PATH="$(dirname "$0")/../data/seeds"

echo "📤 Uploading seeds para $DBFS_PATH..."

# Cria diretório no DBFS
databricks fs mkdirs "$DBFS_PATH" 2>/dev/null || true

for FILE in "$LOCAL_PATH"/*.json; do
  BASENAME=$(basename "$FILE")
  echo "   → $BASENAME"
  databricks fs cp "$FILE" "$DBFS_PATH/$BASENAME" --overwrite
done

echo ""
echo "✅ Upload concluído. Arquivos em $DBFS_PATH:"
databricks fs ls "$DBFS_PATH"

echo ""
echo "ℹ️  No notebook Bronze, use:"
echo "   spark.read.json('$DBFS_PATH/fifa_rankings.json')"
