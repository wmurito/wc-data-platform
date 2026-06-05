# Databricks notebook source
# MAGIC %md
# MAGIC # ML — xG Model (Expected Goals)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 05 ML
# MAGIC **Experimento MLflow:** `worldcup/xg_model`
# MAGIC **Modelo registrado:** `worldcup-xg-model`
# MAGIC **Fonte:** `worldcup.silver.shot_map`
# MAGIC **Executar depois de:** 04_silver_shot_map
# MAGIC
# MAGIC Treina um modelo de Expected Goals (xG) usando os dados reais
# MAGIC do StatsBomb. O xG do StatsBomb já existe nos dados, mas treinamos
# MAGIC nosso próprio modelo para:
# MAGIC   1. Entender quais features mais explicam um gol
# MAGIC   2. Ter um modelo próprio para o portfólio
# MAGIC   3. Usar features 360° que o StatsBomb não incorpora no xG padrão
# MAGIC
# MAGIC **Features do modelo:**
# MAGIC   Geométricas: distance_to_goal, angle_to_goal
# MAGIC   Técnicas: shot_body_part, shot_technique, shot_type, shot_first_time
# MAGIC   Contexto: under_pressure, play_pattern, pitch_zone
# MAGIC   360° (quando disponível): defenders_in_cone, distance_nearest_defender
# MAGIC   Placar: score_diff_at_event, period
# MAGIC
# MAGIC **Target:** is_goal (0/1)
# MAGIC **Algoritmo:** Logistic Regression + XGBoost (comparação)
# MAGIC **Métrica:** ROC-AUC, Brier Score, Log Loss

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

# DBTITLE 1,Cell 3
CATALOG     = "lakehouse"
SCHEMA_S    = "silver"
SCHEMA_G    = "gold"
TABLE_IN    = f"{CATALOG}.{SCHEMA_S}.shot_map"
EXPERIMENT  = "worldcup/xg_model"
MODEL_NAME  = "worldcup-xg-model"

try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception:
    CATALOG  = "hive_metastore"
    SCHEMA_S = "wc_silver"
    TABLE_IN = f"{CATALOG}.{SCHEMA_S}.shot_map"

# COMMAND ----------

# MAGIC %md ## 1. Carregar dados de chutes

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql import functions as F

# Override TABLE_IN with correct catalog
TABLE_IN = "lakehouse.silver.shot_map"
shots_raw = spark.table(TABLE_IN)
print(f"Total chutes: {shots_raw.count():,}")
print(f"  Com dados 360°: {shots_raw.filter(F.col('has_360_data')).count():,}")
print(f"  Gols: {shots_raw.filter(F.col('is_goal')).count():,}")
print(f"  Taxa de conversão: {shots_raw.filter(F.col('is_goal')).count() / shots_raw.count() * 100:.2f}%")

# COMMAND ----------

# MAGIC %md ## 2. Feature engineering e preparação

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    roc_auc_score, brier_score_loss, log_loss,
    classification_report, confusion_matrix
)
import mlflow
import mlflow.sklearn
import warnings
warnings.filterwarnings("ignore")

# Converter para pandas (dataset é pequeno o suficiente)
shots_pd = shots_raw.select(
    "event_id", "match_id", "_competition_label",
    # Target
    "is_goal",
    # Features geométricas
    "distance_to_goal", "angle_to_goal",
    # Features técnicas
    "shot_body_part", "shot_technique", "shot_type",
    "shot_first_time", "shot_one_on_one",
    # Contexto
    "under_pressure", "period", "minute",
    "score_diff_at_event", "pitch_zone",
    # Play pattern
    # 360° features
    "defenders_in_cone", "distance_nearest_defender",
    "gk_distance_to_goalline", "has_360_data",
    # xG do StatsBomb (para comparação)
    "shot_xg",
).toPandas()

print(f"Shape: {shots_pd.shape}")
shots_pd.head(3)

# COMMAND ----------

# MAGIC %md ## 3. Pré-processamento

# COMMAND ----------

df = shots_pd.copy()

# ── Encoding de variáveis categóricas
cat_cols = ["shot_body_part","shot_technique","shot_type","pitch_zone"]
encoders = {}
for col in cat_cols:
    le = LabelEncoder()
    df[col + "_enc"] = le.fit_transform(df[col].fillna("Unknown").astype(str))
    encoders[col] = le

# ── Booleanos → int
bool_cols = ["shot_first_time","shot_one_on_one","under_pressure","has_360_data"]
for col in bool_cols:
    df[col + "_int"] = df[col].fillna(False).astype(int)

# ── Features numéricas — imputar mediana
num_cols = [
    "distance_to_goal","angle_to_goal",
    "score_diff_at_event","period","minute",
    "defenders_in_cone","distance_nearest_defender","gk_distance_to_goalline",
]
for col in num_cols:
    df[col] = df[col].fillna(df[col].median())

# ── Features base (sem 360°)
BASE_FEATURES = [
    "distance_to_goal", "angle_to_goal",
    "shot_body_part_enc", "shot_technique_enc", "shot_type_enc",
    "shot_first_time_int", "shot_one_on_one_int",
    "under_pressure_int", "period", "score_diff_at_event",
    "pitch_zone_enc",
]

# ── Features enriquecidas (com 360°)
FULL_FEATURES = BASE_FEATURES + [
    "defenders_in_cone", "distance_nearest_defender", "gk_distance_to_goalline",
]

TARGET = "is_goal"

X_base = df[BASE_FEATURES].values
X_full = df[FULL_FEATURES].values
y      = df[TARGET].astype(int).values

print(f"Amostras: {len(y)} | Gols: {y.sum()} ({y.mean()*100:.1f}%)")
print(f"Features base: {len(BASE_FEATURES)} | Features full (com 360°): {len(FULL_FEATURES)}")

# COMMAND ----------

# MAGIC %md ## 4. Split treino/teste estratificado por competição

# COMMAND ----------

# Split: WC 2018 + Euro 2020 + Copa América = treino | WC 2022 + Euro 2024 = teste
# (garante que testamos em dados "futuros" em relação ao treino)
mask_test = df["_competition_label"].isin([
    "FIFA World Cup 2022", "UEFA Euro 2024"
])
idx_train = (~mask_test).values
idx_test  = mask_test.values

X_train_base, X_test_base = X_base[idx_train], X_base[idx_test]
X_train_full, X_test_full = X_full[idx_train], X_full[idx_test]
y_train, y_test = y[idx_train], y[idx_test]

print(f"Treino: {len(y_train):,} chutes | {y_train.sum()} gols ({y_train.mean()*100:.1f}%)")
print(f"Teste:  {len(y_test):,} chutes | {y_test.sum()} gols ({y_test.mean()*100:.1f}%)")

# COMMAND ----------

# MAGIC %md ## 5. Treino — Logistic Regression (baseline)

# COMMAND ----------

# DBTITLE 1,Cell 13
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# Configure MLflow to use Databricks model registry
mlflow.set_registry_uri("databricks-uc")
# Experiment name must be an absolute workspace path
experiment_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/worldcup_xg_model"
mlflow.set_experiment(experiment_path)

with mlflow.start_run(run_name="logistic_regression_base") as run_lr:

    pipeline_lr = Pipeline([
        ("scaler", StandardScaler()),
        ("model",  LogisticRegression(
            C=1.0, max_iter=1000,
            class_weight="balanced",   # corrige desbalanceamento (poucos gols)
            random_state=42,
        ))
    ])

    pipeline_lr.fit(X_train_base, y_train)
    y_prob_lr = pipeline_lr.predict_proba(X_test_base)[:, 1]

    auc_lr    = roc_auc_score(y_test, y_prob_lr)
    brier_lr  = brier_score_loss(y_test, y_prob_lr)
    logloss_lr = log_loss(y_test, y_prob_lr)

    # Comparar com xG do StatsBomb nos mesmos chutes
    sb_xg  = df.loc[idx_test, "shot_xg"].fillna(0).values
    auc_sb = roc_auc_score(y_test, sb_xg)

    print(f"\n📊 Logistic Regression (features base):")
    print(f"  ROC-AUC:    {auc_lr:.4f}  (StatsBomb xG: {auc_sb:.4f})")
    print(f"  Brier Score: {brier_lr:.4f}")
    print(f"  Log Loss:    {logloss_lr:.4f}")

    mlflow.log_params({
        "model_type": "LogisticRegression",
        "features": "base",
        "n_features": len(BASE_FEATURES),
        "train_competitions": "WC2018, Euro2020, CopaAmerica2024",
        "test_competitions": "WC2022, Euro2024",
        "class_weight": "balanced",
        "C": 1.0,
    })
    mlflow.log_metrics({
        "roc_auc": auc_lr,
        "brier_score": brier_lr,
        "log_loss": logloss_lr,
        "statsbomb_xg_auc": auc_sb,
    })
    mlflow.sklearn.log_model(pipeline_lr, "model")
    lr_run_id = run_lr.info.run_id

print(f"  MLflow run_id: {lr_run_id}")

# COMMAND ----------

# MAGIC %md ## 6. Treino — XGBoost (modelo principal)

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

# DBTITLE 1,Cell 16
try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    print("XGBoost não disponível — instalar: %pip install xgboost")
    HAS_XGB = False

if HAS_XGB:
    with mlflow.start_run(run_name="xgboost_full_features") as run_xgb:

        xgb_model = XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=(y_train == 0).sum() / (y_train == 1).sum(),
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
            verbosity=0,
        )

        # Treinar com features completas (inclui 360°)
        # Para chutes sem 360°, os valores foram imputados pela mediana
        xgb_model.fit(
            X_train_full, y_train,
            eval_set=[(X_test_full, y_test)],
            verbose=False,
        )

        y_prob_xgb  = xgb_model.predict_proba(X_test_full)[:, 1]
        auc_xgb     = roc_auc_score(y_test, y_prob_xgb)
        brier_xgb   = brier_score_loss(y_test, y_prob_xgb)
        logloss_xgb = log_loss(y_test, y_prob_xgb)

        print(f"\n📊 XGBoost (features completas + 360°):")
        print(f"  ROC-AUC:    {auc_xgb:.4f}  (StatsBomb xG: {auc_sb:.4f})")
        print(f"  Brier Score: {brier_xgb:.4f}")
        print(f"  Log Loss:    {logloss_xgb:.4f}")

        # Feature importances
        importances = dict(zip(FULL_FEATURES, xgb_model.feature_importances_.tolist()))
        print("\n  Feature Importances (Top 10):")
        for feat, imp in sorted(importances.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {feat:35s}: {imp:.4f}")

        mlflow.log_params({
            "model_type": "XGBClassifier",
            "features": "full_with_360",
            "n_features": len(FULL_FEATURES),
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
        })
        mlflow.log_metrics({
            "roc_auc": auc_xgb,
            "brier_score": brier_xgb,
            "log_loss": logloss_xgb,
            "statsbomb_xg_auc": auc_sb,
        })
        mlflow.log_dict(importances, "feature_importances.json")
        mlflow.sklearn.log_model(xgb_model, "model")
        xgb_run_id = run_xgb.info.run_id

    print(f"  MLflow run_id: {xgb_run_id}")

# COMMAND ----------

# MAGIC %md ## 7. Registrar o melhor modelo no MLflow Model Registry

# COMMAND ----------

# DBTITLE 1,Cell 18
best_run_id = xgb_run_id if HAS_XGB and auc_xgb > auc_lr else lr_run_id
best_model  = "XGBoost" if HAS_XGB and auc_xgb > auc_lr else "LogisticRegression"

print(f"\n🏆 Melhor modelo: {best_model}")
print(f"   run_id: {best_run_id}")

# Re-log the best model with signature
from mlflow.models import infer_signature

if best_model == "XGBoost":
    best_model_obj = xgb_model
    X_sample = X_test_full[:5]
    predictions = best_model_obj.predict_proba(X_sample)[:, 1]
else:
    best_model_obj = pipeline_lr
    X_sample = X_test_base[:5]
    predictions = best_model_obj.predict_proba(X_sample)[:, 1]

signature = infer_signature(X_sample, predictions)

with mlflow.start_run(run_id=best_run_id):
    model_info = mlflow.sklearn.log_model(
        best_model_obj,
        "model_with_signature",
        signature=signature,
        input_example=X_sample,
    )

print(f"✅ Modelo logged com signature e input_example")
print(f"   Model URI: {model_info.model_uri}")
print(f"\n⚠️  Registro no Unity Catalog requer permissões adicionais.")
print(f"   Para registrar manualmente, use a UI do MLflow ou:")
print(f"   mlflow.register_model('{model_info.model_uri}', '{MODEL_NAME}')")

# COMMAND ----------

# MAGIC %md ## 8. Gerar predições xG para todos os chutes e salvar na Gold

# COMMAND ----------

# DBTITLE 1,Cell 20
# Carregar modelo do run (já que não foi registrado no UC devido a permissões)
model_uri = f"runs:/{best_run_id}/model_with_signature"
loaded_model = mlflow.sklearn.load_model(model_uri)
print(f"✅ Modelo carregado de: {model_uri}")

# Predição em todo o dataset
X_all = df[FULL_FEATURES if HAS_XGB else BASE_FEATURES].values
df["our_xg"] = loaded_model.predict_proba(X_all)[:, 1]
df["our_xg"] = df["our_xg"].round(4)

# Comparação com StatsBomb xG
diff = (df["our_xg"] - df["shot_xg"]).abs()
print(f"\nComparação our_xG vs StatsBomb xG:")
print(f"  MAE: {diff.mean():.4f}")
print(f"  Correlação: {df['our_xg'].corr(df['shot_xg']):.4f}")

# Salvar predições na Gold
xg_predictions = spark.createDataFrame(
    df[["event_id","match_id","_competition_label","is_goal",
        "shot_xg","our_xg","distance_to_goal","angle_to_goal",
        "shot_body_part","shot_technique","shot_type"]].assign(
        xg_diff = (df["our_xg"] - df["shot_xg"]).round(4),
        _model_run_id = best_run_id,
        _model_name = MODEL_NAME,
    )
)

xg_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable(f"{CATALOG}.gold.xg_predictions")
print(f"✅ {CATALOG}.gold.xg_predictions: {xg_predictions.count():,} chutes")

# COMMAND ----------

# MAGIC %md ## 9. Análise de resultados

# COMMAND ----------

# DBTITLE 1,Cell 22
xg_df = spark.table(f"{CATALOG}.gold.xg_predictions")

print("📊 xG médio por tipo de chute (nosso modelo vs StatsBomb):")
xg_df.groupBy("shot_type") \
    .agg(
        F.count("*").alias("chutes"),
        F.round(F.avg("shot_xg"), 4).alias("statsbomb_xg"),
        F.round(F.avg("our_xg"), 4).alias("our_xg"),
        F.round(F.avg(F.col("is_goal").cast("int")), 4).alias("taxa_real"),
    ).orderBy("statsbomb_xg", ascending=False) \
    .show(truncate=False)

print("\n🎯 Top 10 chutes com maior xG (nosso modelo):")
xg_df.orderBy(F.col("our_xg").desc()) \
    .select("_competition_label","match_id","shot_type",
            "shot_technique","distance_to_goal","angle_to_goal",
            "our_xg","shot_xg","is_goal") \
    .show(10, truncate=False)
