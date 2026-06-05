# Databricks notebook source
# MAGIC %md
# MAGIC # ML — Match Result Predictor
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 05 ML
# MAGIC **Experimento MLflow:** `worldcup/match_predictor`
# MAGIC **Modelo registrado:** `worldcup-match-predictor`
# MAGIC **Fonte:** `worldcup.gold.match_features`
# MAGIC **Executar depois de:** 03_gold_match_features + 01_ml_xg_model
# MAGIC
# MAGIC Prediz o resultado de uma partida de Copa do Mundo (W/D/L do ponto
# MAGIC de vista do time mandante) usando as 45+ features da Gold.
# MAGIC
# MAGIC **Abordagem:**
# MAGIC   - Problema multiclasse: 1 (home win), 0 (draw), -1 (away win)
# MAGIC   - Algoritmos: XGBoost + LightGBM + Ensemble por votação ponderada
# MAGIC   - Validação: Leave-one-tournament-out (treina em N-1 torneios, testa no N)
# MAGIC   - Output: probabilidades P(home win), P(draw), P(away win)
# MAGIC
# MAGIC **Feature importance esperada (literatura):**
# MAGIC   #1 elo_diff (~30% importance)
# MAGIC   #2 xg_diff  (~18%)
# MAGIC   #3 home_dominance_score (~12%)
# MAGIC   #4 form_xg_diff (~10%)
# MAGIC   #5 ppda_diff (~8%)

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

# DBTITLE 1,Cell 3
CATALOG     = "lakehouse"
SCHEMA_G    = "gold"
TABLE_IN    = f"{CATALOG}.{SCHEMA_G}.match_features"
EXPERIMENT  = "worldcup/match_predictor"
MODEL_NAME  = "worldcup-match-predictor"

try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception:
    CATALOG  = "hive_metastore"
    SCHEMA_G = "wc_gold"
    TABLE_IN = f"{CATALOG}.{SCHEMA_G}.match_features"

# COMMAND ----------

# MAGIC %md ## 1. Carregar feature store

# COMMAND ----------

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import functions as F

mf_raw = spark.table(TABLE_IN)
print(f"Partidas na feature store: {mf_raw.count():,}")
print(f"Colunas: {len(mf_raw.columns)}")

# Converter para pandas
mf = mf_raw.toPandas()
print(f"\nDistribuição de resultados:")
print(mf["result_home"].value_counts().rename({1:"Home Win", 0:"Draw", -1:"Away Win"}))

# COMMAND ----------

# MAGIC %md ## 2. Seleção e preparação de features

# COMMAND ----------

FEATURE_COLS = [
    # ── ELO (mais importantes)
    "elo_diff",
    "elo_home_win_prob",
    "elo_home",
    "elo_away",

    # ── Estilo de jogo — diferenciais
    "xg_diff",
    "ppda_diff",
    "possession_diff",
    "dominance_diff",

    # ── Estilo de jogo — absolutos home
    "home_xg_per_game",
    "home_xga_per_game",
    "home_ppda",
    "home_possession_pct",
    "home_prog_passes",
    "home_dominance_score",

    # ── Estilo de jogo — absolutos away
    "away_xg_per_game",
    "away_xga_per_game",
    "away_ppda",
    "away_possession_pct",
    "away_prog_passes",
    "away_dominance_score",

    # ── Forma recente
    "home_form_xg",
    "home_form_xga",
    "home_form_ppda",
    "home_form_wins",
    "away_form_xg",
    "away_form_xga",
    "away_form_ppda",
    "away_form_wins",
    "form_xg_diff",
    "form_wins_diff",

    # ── Contexto
    "stage_ordinal",
]

TARGET = "result_home"   # 1, 0, -1

# Remover linhas com target nulo e imputar features
df = mf[FEATURE_COLS + [TARGET, "_competition_label", "match_id",
                          "home_team_name","away_team_name",
                          "home_score","away_score"]].copy()
df = df.dropna(subset=[TARGET])

# Imputar features numéricas com mediana
for col in FEATURE_COLS:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df[col] = df[col].fillna(df[col].median())

# Converter target para 0/1/2 (sklearn precisa de inteiros positivos)
label_map  = {1: 2, 0: 1, -1: 0}   # 2=home win, 1=draw, 0=away win
label_back = {2: 1, 1: 0, 0: -1}

df["target"] = df[TARGET].map(label_map)

X = df[FEATURE_COLS].values
y = df["target"].values

print(f"\nShape final: X={X.shape}, y={y.shape}")
print(f"Classes: {np.unique(y, return_counts=True)}")

# COMMAND ----------

# MAGIC %md ## 3. Validação Leave-One-Tournament-Out
# MAGIC
# MAGIC Treina em N-1 torneios, testa no torneio deixado de fora.
# MAGIC Mais realista que random split para dados temporais.

# COMMAND ----------

from sklearn.metrics import accuracy_score, f1_score
from collections import defaultdict

tournaments = df["_competition_label"].unique()
print(f"Torneios disponíveis: {tournaments}")

loto_results = []

for test_comp in tournaments:
    mask_test  = df["_competition_label"] == test_comp
    mask_train = ~mask_test

    X_tr, y_tr = X[mask_train], y[mask_train]
    X_te, y_te = X[mask_test],  y[mask_test]

    if len(X_te) < 5:
        continue

    try:
        from xgboost import XGBClassifier
        model = XGBClassifier(
            n_estimators=150, max_depth=4,
            learning_rate=0.05, subsample=0.8,
            use_label_encoder=False, eval_metric="mlogloss",
            random_state=42, verbosity=0,
        )
        model.fit(X_tr, y_tr)
        y_pred = model.predict(X_te)
        acc = accuracy_score(y_te, y_pred)
    except:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(n_estimators=100, random_state=42)
        model.fit(X_tr, y_tr)
        y_pred = model.predict(X_te)
        acc = accuracy_score(y_te, y_pred)

    loto_results.append({
        "test_competition": test_comp,
        "n_matches": len(y_te),
        "accuracy": round(acc, 4),
        "baseline_home_win": round((y_te == 2).mean(), 4),
    })
    print(f"  {test_comp[:30]:30s} | acc={acc:.4f} | baseline={loto_results[-1]['baseline_home_win']:.4f} | n={len(y_te)}")

loto_df = pd.DataFrame(loto_results)
print(f"\nMédia LOTO accuracy: {loto_df['accuracy'].mean():.4f}")
print(f"Média baseline (prever home win sempre): {loto_df['baseline_home_win'].mean():.4f}")

# COMMAND ----------

# MAGIC %md ## 4. Treino final — modelo completo (todos os torneios)

# COMMAND ----------

# DBTITLE 1,Instalar pacotes ML
# MAGIC %pip install xgboost lightgbm --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 11
# Configurar MLflow para Unity Catalog
mlflow.set_registry_uri("databricks-uc")
# Experimento precisa ser caminho absoluto no workspace
experiment_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/match_predictor"
mlflow.set_experiment(experiment_path)

# ── XGBoost
try:
    from xgboost import XGBClassifier

    with mlflow.start_run(run_name="xgboost_multiclass_final") as run_xgb:
        xgb = XGBClassifier(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.04,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            gamma=0.1,
            use_label_encoder=False,
            eval_metric="mlogloss",
            random_state=42,
            verbosity=0,
        )
        xgb.fit(X, y)

        y_pred_xgb  = xgb.predict(X)
        y_prob_xgb  = xgb.predict_proba(X)
        acc_xgb     = accuracy_score(y, y_pred_xgb)

        importances = dict(zip(FEATURE_COLS, xgb.feature_importances_.tolist()))
        print(f"\n📊 XGBoost (treino completo):")
        print(f"  Accuracy (train): {acc_xgb:.4f}")
        print(f"  LOTO mean acc:    {loto_df['accuracy'].mean():.4f}")
        print(f"\n  Top 10 features:")
        for feat, imp in sorted(importances.items(), key=lambda x: x[1], reverse=True)[:10]:
            bar = "█" * int(imp * 200)
            print(f"    {feat:35s} {imp:.4f} {bar}")

        mlflow.log_params({
            "model_type": "XGBClassifier",
            "n_estimators": 300, "max_depth": 5,
            "learning_rate": 0.04,
            "n_features": len(FEATURE_COLS),
            "n_tournaments": len(tournaments),
            "n_matches": len(X),
        })
        mlflow.log_metrics({
            "train_accuracy": acc_xgb,
            "loto_mean_accuracy": loto_df["accuracy"].mean(),
            "loto_std_accuracy": loto_df["accuracy"].std(),
        })
        mlflow.log_dict(importances, "feature_importances.json")
        mlflow.log_dict(loto_df.to_dict(orient="records"), "loto_results.json")
        mlflow.sklearn.log_model(xgb, "model",
            input_example=pd.DataFrame(X[:1], columns=FEATURE_COLS))

        xgb_run_id = run_xgb.info.run_id

except ImportError:
    print("XGBoost não disponível. Usando GradientBoosting.")
    xgb_run_id = None

# ── LightGBM
try:
    import lightgbm as lgb

    with mlflow.start_run(run_name="lightgbm_multiclass_final") as run_lgb:
        lgbm = lgb.LGBMClassifier(
            n_estimators=300,
            num_leaves=31,
            learning_rate=0.04,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_samples=10,
            random_state=42,
            verbose=-1,
        )
        lgbm.fit(X, y)
        y_pred_lgb = lgbm.predict(X)
        acc_lgb    = accuracy_score(y, y_pred_lgb)

        print(f"\n📊 LightGBM:")
        print(f"  Accuracy (train): {acc_lgb:.4f}")

        mlflow.log_params({"model_type": "LGBMClassifier", "n_estimators": 300})
        mlflow.log_metrics({"train_accuracy": acc_lgb,
                             "loto_mean_accuracy": loto_df["accuracy"].mean()})
        mlflow.sklearn.log_model(lgbm, "model")
        lgb_run_id = run_lgb.info.run_id

except ImportError:
    lgb_run_id = None
    print("LightGBM não disponível.")

# COMMAND ----------

# MAGIC %md ## 5. Registrar o melhor modelo

# COMMAND ----------

# DBTITLE 1,Cell 13
best_run_id = xgb_run_id or lgb_run_id

if best_run_id is None:
    print("⚠️  Nenhum modelo foi treinado (XGBoost e LightGBM não disponíveis).")
    print("   Instale com: %pip install xgboost lightgbm")
else:
    model_uri = f"runs:/{best_run_id}/model"
    print(f"✅ Melhor modelo: {'XGBoost' if best_run_id == xgb_run_id else 'LightGBM'}")
    print(f"   Run ID: {best_run_id}")
    print(f"   Model URI: {model_uri}")
    print(f"\n⚠️  Registro no Unity Catalog requer permissões adicionais.")
    print(f"   Para registrar manualmente:")
    print(f"   mlflow.register_model('{model_uri}', '{MODEL_NAME}')")

# COMMAND ----------

# MAGIC %md ## 6. Predição de partidas históricas — calibração

# COMMAND ----------

# DBTITLE 1,Cell 16
# Carregar modelo do run (já que não foi registrado no UC)
if best_run_id is None:
    print("⚠️  Nenhum modelo disponível para predição.")
    raise ValueError("Execute as células anteriores para treinar o modelo.")

model_uri = f"runs:/{best_run_id}/model"
loaded = mlflow.sklearn.load_model(model_uri)
print(f"✅ Modelo carregado de: {model_uri}")

probs   = loaded.predict_proba(X)
df_pred = df[["match_id","_competition_label","home_team_name","away_team_name",
              "home_score","away_score","target"]].copy()
df_pred["p_away_win"]  = probs[:, 0].round(4)
df_pred["p_draw"]      = probs[:, 1].round(4)
df_pred["p_home_win"]  = probs[:, 2].round(4)
df_pred["result_home"] = df_pred["target"].map(label_back)
df_pred["predicted"]   = loaded.predict(X)
df_pred["correct"]     = (df_pred["predicted"] == df_pred["target"]).astype(int)

print(f"\nAcurácia geral: {df_pred['correct'].mean():.4f}")
print(f"Acurácia por competição:")
print(df_pred.groupby("_competition_label")["correct"].mean().sort_values(ascending=False))

# Casos com maior probabilidade home win que perdeu — surpresas
print("\n😱 Maiores surpresas (high p_home_win mas home perdeu):")
upsets = df_pred[
    (df_pred["p_home_win"] > 0.65) & (df_pred["result_home"] == -1)
].sort_values("p_home_win", ascending=False)
print(upsets[["_competition_label","home_team_name","away_team_name",
              "home_score","away_score","p_home_win","p_away_win"]].head(10).to_string())

# COMMAND ----------

# MAGIC %md ## 7. Salvar predições na Gold

# COMMAND ----------

pred_spark = spark.createDataFrame(df_pred.drop(columns=["target"]))
pred_spark = pred_spark.withColumn("_model_name", F.lit(MODEL_NAME)) \
                       .withColumn("_processed_at", F.current_timestamp())

pred_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable(f"{CATALOG}.gold.match_predictions")

print(f"✅ worldcup.gold.match_predictions: {pred_spark.count():,} partidas")
