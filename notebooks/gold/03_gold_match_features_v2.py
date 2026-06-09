# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Match Features v2 (com Squad Quality)
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 04 Gold — Enriquecimento
# MAGIC **Tabela:** `worldcup.gold.match_features_v2`
# MAGIC **Fonte:** gold.match_features (v1) + silver.squad_features
# MAGIC **Executar depois de:** 06_silver_squad_features + 03_gold_match_features
# MAGIC
# MAGIC Adiciona 25+ novas features de qualidade de elenco ao feature store.
# MAGIC
# MAGIC NOVAS FEATURES vs v1:
# MAGIC
# MAGIC  Ratings SoFIFA (home e away):
# MAGIC    home_squad_avg_overall, away_squad_avg_overall
# MAGIC    home_squad_top11_overall, away_squad_top11_overall
# MAGIC    home_att_best_overall, away_att_best_overall
# MAGIC    home_def_avg_overall, away_def_avg_overall
# MAGIC    home_gk_best_overall, away_gk_best_overall
# MAGIC    overall_diff (home - away)
# MAGIC    att_overall_diff
# MAGIC
# MAGIC  Perfil de risco etário:
# MAGIC    home_squad_avg_age, away_squad_avg_age
# MAGIC    home_squad_peak_pct, away_squad_peak_pct
# MAGIC    home_squad_veteran_pct, away_squad_veteran_pct
# MAGIC    age_diff (home - away)
# MAGIC    peak_pct_diff
# MAGIC
# MAGIC  Forma recente (últimas 10 partidas):
# MAGIC    home_form_ppg_recent, away_form_ppg_recent
# MAGIC    home_form_xg_diff_recent, away_form_xg_diff_recent
# MAGIC    home_form_clean_sheet_pct
# MAGIC    recent_form_ppg_diff
# MAGIC
# MAGIC  Valor de mercado:
# MAGIC    home_squad_value_log, away_squad_value_log
# MAGIC    squad_value_diff
# MAGIC    home_depth_score, away_depth_score

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
CATALOG    = "worldcup"
SCHEMA_S   = "silver"
SCHEMA_G   = "gold"
TABLE_IN   = f"{CATALOG}.{SCHEMA_G}.match_features"
TABLE_SQ   = f"{CATALOG}.{SCHEMA_S}.squad_features"
TABLE_OUT  = f"{CATALOG}.{SCHEMA_G}.match_features_v2"

try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception:
    CATALOG    = "hive_metastore"
    SCHEMA_S   = "wc_silver"
    SCHEMA_G   = "wc_gold"
    TABLE_IN   = f"{CATALOG}.{SCHEMA_G}.match_features"
    TABLE_SQ   = f"{CATALOG}.{SCHEMA_S}.squad_features"
    TABLE_OUT  = f"{CATALOG}.{SCHEMA_G}.match_features_v2"

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow
import mlflow.sklearn

# COMMAND ----------
# MAGIC %md ## 1. Carregar base e squad features

# COMMAND ----------
mf_v1 = spark.table(TABLE_IN)
squad  = spark.table(TABLE_SQ)

print(f"Match features v1: {mf_v1.count():,} partidas | {len(mf_v1.columns)} features")
print(f"Squad features: {squad.count():,} seleções | {len(squad.columns)} features")

# COMMAND ----------
# MAGIC %md ## 2. Preparar squad features para join home e away

# COMMAND ----------
# Selecionar colunas de squad relevantes para o modelo
SQUAD_COLS = [
    "team_name",
    # Ratings
    "squad_avg_overall", "squad_top11_overall", "squad_best_player_overall",
    "squad_avg_potential", "squad_depth_score", "squad_overall_std",
    "gk_best_overall", "def_avg_overall", "mid_avg_overall",
    "att_avg_overall", "att_best_overall",
    # Idade / perfil
    "squad_avg_age", "squad_top11_avg_age",
    "squad_peak_pct", "squad_veteran_pct", "squad_young_pct",
    # Forma recente (squad)
    "form_ppg", "form_xg_per_game", "form_xga_per_game",
    "form_xg_diff", "form_win_rate", "form_clean_sheet_pct",
    # Mercado
    "squad_value_m", "squad_value_log",
    "avg_player_value_m", "top3_value_m",
    "n_players_over_50m", "depth_value_ratio",
]

# Só pegar colunas que existem
available_cols = ["team_name"] + [c for c in SQUAD_COLS[1:] if c in squad.columns]
squad_slim = squad.select(*available_cols)

# Home features
squad_home = squad_slim.select(
    [F.col("team_name").alias("home_team_name")] +
    [F.col(c).alias(f"home_{c}") for c in available_cols[1:]]
)

# Away features
squad_away = squad_slim.select(
    [F.col("team_name").alias("away_team_name")] +
    [F.col(c).alias(f"away_{c}") for c in available_cols[1:]]
)

print(f"Squad home features: {len(squad_home.columns)}")
print(f"Squad away features: {len(squad_away.columns)}")

# COMMAND ----------
# MAGIC %md ## 3. Join no feature store

# COMMAND ----------
mf_v2 = mf_v1 \
    .join(squad_home, on="home_team_name", how="left") \
    .join(squad_away, on="away_team_name", how="left")

# COMMAND ----------
# MAGIC %md ## 4. Features diferenciais (home - away) — as mais poderosas para o ML

# COMMAND ----------
mf_v2 = mf_v2 \
    .withColumn("overall_diff",
        F.col("home_squad_avg_overall") - F.col("away_squad_avg_overall")
    ).withColumn("top11_overall_diff",
        F.col("home_squad_top11_overall") - F.col("away_squad_top11_overall")
    ).withColumn("att_overall_diff",
        F.col("home_att_best_overall") - F.col("away_att_best_overall")
    ).withColumn("def_overall_diff",
        F.col("home_def_avg_overall") - F.col("away_def_avg_overall")
    ).withColumn("gk_overall_diff",
        F.col("home_gk_best_overall") - F.col("away_gk_best_overall")
    ).withColumn("age_diff",
        F.col("home_squad_avg_age") - F.col("away_squad_avg_age")
    ).withColumn("peak_pct_diff",
        F.col("home_squad_peak_pct") - F.col("away_squad_peak_pct")
    ).withColumn("veteran_pct_diff",
        F.col("home_squad_veteran_pct") - F.col("away_squad_veteran_pct")
    ).withColumn("recent_form_ppg_diff",
        F.col("home_form_ppg") - F.col("away_form_ppg")
    ).withColumn("recent_xg_diff_diff",
        F.col("home_form_xg_diff") - F.col("away_form_xg_diff")
    ).withColumn("squad_value_diff",
        F.col("home_squad_value_log") - F.col("away_squad_value_log")
    ).withColumn("depth_score_diff",
        F.col("home_depth_score") - F.col("away_depth_score")
    )

# Squad Risk Score composto
# Time com elenco velho + baixo form + ataque fraco = maior risco
mf_v2 = mf_v2 \
    .withColumn("home_squad_risk_score",
        F.round(
            F.col("home_squad_veteran_pct") * 0.3 +
            (100 - F.coalesce(F.col("home_squad_avg_overall"), F.lit(75))) * 0.4 +
            (3 - F.coalesce(F.col("home_form_ppg"), F.lit(1.5))) * 10 * 0.3,
            2
        )
    ).withColumn("away_squad_risk_score",
        F.round(
            F.col("away_squad_veteran_pct") * 0.3 +
            (100 - F.coalesce(F.col("away_squad_avg_overall"), F.lit(75))) * 0.4 +
            (3 - F.coalesce(F.col("away_form_ppg"), F.lit(1.5))) * 10 * 0.3,
            2
        )
    ).withColumn("squad_risk_diff",
        F.col("home_squad_risk_score") - F.col("away_squad_risk_score")
    )

mf_v2 = mf_v2.withColumn("_processed_at", F.current_timestamp())

total = mf_v2.count()
print(f"\nMatch Features v2: {total:,} partidas | {len(mf_v2.columns)} features total")
print(f"Features novas adicionadas: {len(mf_v2.columns) - len(mf_v1.columns)}")

# COMMAND ----------
# MAGIC %md ## 5. Escrita Gold

# COMMAND ----------
mf_v2.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_competition_id") \
    .saveAsTable(TABLE_OUT)

print(f"✅ {TABLE_OUT}: {total:,} partidas | {len(mf_v2.columns)} features")

# COMMAND ----------
# MAGIC %md ## 6. Retreinar o Match Predictor com as novas features

# COMMAND ----------
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

MODEL_NAME  = "worldcup-match-predictor-v2"
EXPERIMENT  = "worldcup/match_predictor_v2"

# Features v1 + novas features de squad
FEATURE_COLS_V2 = [
    # ── ELO (mantidas)
    "elo_diff", "elo_home_win_prob", "elo_home", "elo_away",
    # ── Estilo (mantidas)
    "xg_diff", "ppda_diff", "possession_diff", "dominance_diff",
    "home_xg_per_game", "home_xga_per_game", "home_ppda",
    "home_possession_pct", "home_dominance_score",
    "away_xg_per_game", "away_xga_per_game", "away_ppda",
    "away_possession_pct", "away_dominance_score",
    # ── Forma histórica (mantidas)
    "home_form_xg", "home_form_xga", "home_form_wins",
    "away_form_xg", "away_form_xga", "away_form_wins",
    "form_xg_diff", "form_wins_diff",
    # ── Fase (mantida)
    "stage_ordinal",
    # ── NOVAS: Squad ratings
    "overall_diff", "top11_overall_diff",
    "att_overall_diff", "def_overall_diff", "gk_overall_diff",
    "home_squad_avg_overall", "away_squad_avg_overall",
    "home_att_best_overall", "away_att_best_overall",
    "home_gk_best_overall", "away_gk_best_overall",
    # ── NOVAS: Perfil etário
    "age_diff", "peak_pct_diff", "veteran_pct_diff",
    "home_squad_avg_age", "away_squad_avg_age",
    "home_squad_peak_pct", "away_squad_peak_pct",
    # ── NOVAS: Forma recente das seleções
    "recent_form_ppg_diff", "recent_xg_diff_diff",
    "home_form_ppg", "away_form_ppg",
    "home_form_xg_diff", "away_form_xg_diff",
    "home_form_win_rate", "away_form_win_rate",
    "home_form_clean_sheet_pct",
    # ── NOVAS: Mercado
    "squad_value_diff", "depth_score_diff",
    "home_squad_value_log", "away_squad_value_log",
    # ── NOVAS: Risk score
    "squad_risk_diff", "home_squad_risk_score", "away_squad_risk_score",
]

TARGET = "result_home"

# Carregar dados
mf_pd = mf_v2.select(FEATURE_COLS_V2 + [TARGET, "_competition_label"]).toPandas()
mf_pd = mf_pd.dropna(subset=[TARGET])

# Imputar numéricas
for col in FEATURE_COLS_V2:
    mf_pd[col] = pd.to_numeric(mf_pd[col], errors="coerce")
    mf_pd[col] = mf_pd[col].fillna(mf_pd[col].median())

label_map = {1: 2, 0: 1, -1: 0}
mf_pd["target"] = mf_pd[TARGET].map(label_map)

X = mf_pd[FEATURE_COLS_V2].values
y = mf_pd["target"].values

print(f"\nDataset v2: {X.shape[0]} partidas | {X.shape[1]} features")
print(f"  (v1 tinha ~31 features — adicionamos {X.shape[1]-31} novas)")

# COMMAND ----------
# MAGIC %md ## 7. LOTO Validation — comparar v1 vs v2

# COMMAND ----------
from sklearn.metrics import accuracy_score

try:
    from xgboost import XGBClassifier
    ModelClass = XGBClassifier
    model_params = dict(
        n_estimators=300, max_depth=5, learning_rate=0.04,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
        use_label_encoder=False, eval_metric="mlogloss",
        random_state=42, verbosity=0,
    )
except ImportError:
    from sklearn.ensemble import GradientBoostingClassifier
    ModelClass = GradientBoostingClassifier
    model_params = dict(n_estimators=200, random_state=42)

tournaments = mf_pd["_competition_label"].unique()
loto_v2 = []

print("\n📊 Leave-One-Tournament-Out Validation:")
print(f"{'Torneio':35s} {'Acc v2':>8s} {'N matches':>10s}")
print("─" * 56)

for test_comp in tournaments:
    mask = mf_pd["_competition_label"] == test_comp
    X_tr, y_tr = X[~mask], y[~mask]
    X_te, y_te = X[mask],  y[mask]

    if len(X_te) < 5:
        continue

    model = ModelClass(**model_params)
    model.fit(X_tr, y_tr)
    acc = accuracy_score(y_te, model.predict(X_te))
    loto_v2.append({"competition": test_comp, "accuracy_v2": acc, "n": len(y_te)})
    print(f"  {test_comp[:33]:33s} {acc:.4f}   {len(y_te):>8d}")

loto_v2_df = pd.DataFrame(loto_v2)
print(f"\n  Média LOTO v2: {loto_v2_df['accuracy_v2'].mean():.4f}")
print(f"  (v1 era ~0.58 — esperamos ganho de 3-6 pontos percentuais com squad features)")

# COMMAND ----------
# MAGIC %md ## 8. Treinar modelo final v2 e registrar no MLflow

# COMMAND ----------
mlflow.set_experiment(EXPERIMENT)

with mlflow.start_run(run_name="xgboost_squad_features_final") as run:

    final_model = ModelClass(**model_params)
    final_model.fit(X, y)

    train_acc = accuracy_score(y, final_model.predict(X))
    loto_mean = loto_v2_df["accuracy_v2"].mean()

    # Feature importances
    if hasattr(final_model, "feature_importances_"):
        importances = dict(zip(FEATURE_COLS_V2, final_model.feature_importances_))
        top_features = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:15]

        print(f"\n🔑 Top 15 Features (v2):")
        for feat, imp in top_features:
            bar = "█" * int(imp * 300)
            tag = "🆕" if feat in [
                "overall_diff","top11_overall_diff","att_overall_diff",
                "age_diff","peak_pct_diff","recent_form_ppg_diff",
                "squad_value_diff","squad_risk_diff","recent_xg_diff_diff"
            ] else "  "
            print(f"  {tag} {feat:35s} {imp:.4f} {bar}")

        mlflow.log_dict(importances, "feature_importances_v2.json")

    mlflow.log_params({
        "model_version": "v2",
        "n_features_v2": len(FEATURE_COLS_V2),
        "new_features": "squad_ratings,age_profile,recent_form,market_value",
        "n_matches": len(X),
        "n_tournaments": len(tournaments),
    })
    mlflow.log_metrics({
        "train_accuracy": train_acc,
        "loto_mean_accuracy_v2": loto_mean,
        "loto_std_accuracy_v2": loto_v2_df["accuracy_v2"].std(),
    })
    mlflow.log_dict(loto_v2_df.to_dict(orient="records"), "loto_results_v2.json")
    mlflow.sklearn.log_model(
        final_model, "model",
        input_example=pd.DataFrame(X[:1], columns=FEATURE_COLS_V2)
    )
    run_id = run.info.run_id

# Registrar no Model Registry
model_uri = f"runs:/{run_id}/model"
result = mlflow.register_model(model_uri, MODEL_NAME)
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name=MODEL_NAME, version=result.version,
    stage="Production", archive_existing_versions=True,
)
print(f"\n✅ '{MODEL_NAME}' v{result.version} → Production")
print(f"   LOTO accuracy v2: {loto_mean:.4f}")

# COMMAND ----------
# MAGIC %md ## 9. Atualizar simulador com novo modelo

# COMMAND ----------
print("""
╔══════════════════════════════════════════════════════════════╗
║  ✅ Match Features v2 completo!                              ║
║                                                              ║
║  Próximo passo obrigatório:                                  ║
║  Rodar 03_ml_tournament_simulator.py com MODEL_NAME =        ║
║  "worldcup-match-predictor-v2" para regravar                 ║
║  gold.simulation_results com as novas probabilidades.        ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------
# MAGIC %md ## 10. Validação final — impacto das novas features

# COMMAND ----------
mf2 = spark.table(TABLE_OUT)

print("🧓 Análise de risco etário por seleção (squad_risk_score):")
mf2.filter(F.col("home_squad_avg_age").isNotNull()) \
   .groupBy("home_team_name") \
   .agg(
       F.avg("home_squad_avg_age").alias("avg_age"),
       F.avg("home_squad_peak_pct").alias("peak_pct"),
       F.avg("home_squad_veteran_pct").alias("veteran_pct"),
       F.avg("home_squad_avg_overall").alias("avg_overall"),
       F.avg("home_form_ppg").alias("form_ppg"),
       F.avg("home_squad_risk_score").alias("risk_score"),
   ) \
   .orderBy("risk_score", ascending=False) \
   .show(15, truncate=False)

print("\n📊 Correlação das novas features com resultado:")
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

new_feature_sample = [
    "overall_diff","att_overall_diff","age_diff","peak_pct_diff",
    "recent_form_ppg_diff","squad_value_diff","squad_risk_diff","result_home"
]
available = [c for c in new_feature_sample if c in mf2.columns]

if len(available) > 1:
    assembler = VectorAssembler(inputCols=available, outputCol="vec", handleInvalid="skip")
    vec_df = assembler.transform(mf2.filter(F.col("result_home").isNotNull()))
    corr_matrix = Correlation.corr(vec_df, "vec").collect()[0][0]
    result_idx  = available.index("result_home")
    print(f"\n  Correlação com result_home:")
    for i, feat in enumerate(available[:-1]):
        print(f"    {feat:35s}: {corr_matrix[i, result_idx]:.4f}")
