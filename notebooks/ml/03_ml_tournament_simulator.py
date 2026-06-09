# Databricks notebook source
# MAGIC %md
# MAGIC # ML — Monte Carlo Tournament Simulator
# MAGIC **Projeto:** WorldCup Data Platform | **Fase:** 05 ML
# MAGIC **Experimento MLflow:** `worldcup/tournament_simulator`
# MAGIC **Tabela:** `worldcup.gold.simulation_results`
# MAGIC **Fonte:** `worldcup.silver.elo_current_snapshot` + `worldcup.gold.team_style_overall` + modelo de produção
# MAGIC **Executar depois de:** 02_ml_match_predictor
# MAGIC
# MAGIC Simula a Copa do Mundo 2026 (48 times, 104 jogos) usando Monte Carlo.
# MAGIC
# MAGIC **Metodologia:**
# MAGIC   1. Para cada jogo simulado, usa o match predictor para obter P(W), P(D), P(L)
# MAGIC   2. Sorteia o resultado baseado nessas probabilidades
# MAGIC   3. Calcula classificação da fase de grupos (critérios FIFA)
# MAGIC   4. Simula mata-mata até a final
# MAGIC   5. Repete N_SIMULATIONS vezes
# MAGIC   6. Agrega % de vezes que cada seleção atingiu cada fase
# MAGIC
# MAGIC **Copa 2026 — formato:**
# MAGIC   - 48 seleções, 12 grupos de 4
# MAGIC   - Top 2 de cada grupo + 8 melhores terceiros = 32 times
# MAGIC   - Fase de mata-mata: oitavas → quartas → semis → final
# MAGIC   - Sede: EUA / México / Canadá

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

CATALOG       = "worldcup"
SCHEMA_S      = "silver"
SCHEMA_G      = "gold"
MODEL_NAME    = "worldcup-match-predictor"
N_SIMULATIONS = 10_000    # número de simulações Monte Carlo

try:
    spark.sql(f"USE CATALOG {CATALOG}")
except Exception:
    CATALOG  = "hive_metastore"
    SCHEMA_S = "wc_silver"
    SCHEMA_G = "wc_gold"

# COMMAND ----------

# MAGIC %md ## 1. Carregar ELO e estilo de jogo das seleções

# COMMAND ----------

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from pyspark.sql import functions as F
from collections import defaultdict

# ELO atual de cada seleção - obter ELO mais recente por time
elo_home = spark.table("lakehouse.silver.elo_ratings") \
    .select(F.col("home_team").alias("team_name"), "elo_home_after", "match_date") \
    .withColumnRenamed("elo_home_after", "elo_rating")
    
elo_away = spark.table("lakehouse.silver.elo_ratings") \
    .select(F.col("away_team").alias("team_name"), "elo_away_after", "match_date") \
    .withColumnRenamed("elo_away_after", "elo_rating")

elo_df = elo_home.union(elo_away) \
    .orderBy(F.col("match_date").desc()) \
    .dropDuplicates(["team_name"]) \
    .toPandas().set_index("team_name")["elo_rating"].to_dict()

# Estilo de jogo (médias de todos os torneios)
style_df = spark.table("lakehouse.gold.team_style_overall") \
    .toPandas().set_index("team_name")

print(f"Times com ELO: {len(elo_df)}")
print(f"Times com style: {len(style_df)}")

# COMMAND ----------

# MAGIC %md ## 2. Definição dos grupos da Copa 2026
# MAGIC
# MAGIC Grupos provisórios baseados nos rankings FIFA e confirmações até a data.
# MAGIC Atualizar após o sorteio oficial.

# COMMAND ----------

# 48 times em 12 grupos de 4
# Fonte: estimativa baseada no ranking FIFA e qualificações
WC_2026_GROUPS = {
    "A": ["Mexico",      "South Africa",      "South Korea",      "Czech Republic"],
    "B": ["Canada",      "Bosnia and Herzegovina", "Qatar",     "Switzerland"],
    "C": ["Brazil",      "Morocco",           "Haiti",            "Scotland"],
    "D": ["United States","Paraguay",         "Australia",        "Turkey"],
    "E": ["Germany",     "Curacao",           "Ivory Coast",      "Ecuador"],
    "F": ["Netherlands", "Japan",             "Sweden",           "Tunisia"],
    "G": ["Belgium",     "Egypt",             "Iran",             "New Zealand"],
    "H": ["Spain",       "Cape Verde",        "Saudi Arabia",     "Uruguay"],
    "I": ["France",      "Senegal",           "Iraq",             "Norway"],
    "J": ["Argentina",   "Algeria",           "Austria",          "Jordan"],
    "K": ["Portugal",    "DR Congo",          "Uzbekistan",       "Colombia"],
    "L": ["England",     "Croatia",           "Ghana",            "Panama"],
}

# Normalizar nomes para casar com o ELO
NAME_MAP = {
    "United States": "United States",
    "South Korea": "Korea Republic",
    "Ivory Coast": "Côte d'Ivoire",
    "Czech Republic": "Czechia",
    "Saudi Arabia": "Saudi Arabia",
    "DR Congo": "Congo DR",
    "Cape Verde": "Cape Verde Islands",
    "Bosnia and Herzegovina": "Bosnia-Herzegovina",
}

ALL_TEAMS = [t for grp in WC_2026_GROUPS.values() for t in grp]
print(f"Times na Copa 2026: {len(ALL_TEAMS)}")

# ELO padrão para times sem histórico
DEFAULT_ELO = 1050.0
DEFAULT_XG  = 1.2
DEFAULT_XGA = 1.3
DEFAULT_PPDA = 12.0

def get_elo(team: str) -> float:
    mapped = NAME_MAP.get(team, team)
    return elo_df.get(mapped, elo_df.get(team, DEFAULT_ELO))

def get_style(team: str, field: str, default: float) -> float:
    mapped = NAME_MAP.get(team, team)
    for name in [mapped, team]:
        if name in style_df.index and field in style_df.columns:
            val = style_df.loc[name, field]
            if not pd.isna(val):
                return float(val)
    return default

# COMMAND ----------

# MAGIC %md ## 3. Funções de predição e simulação

# COMMAND ----------

# Carregar modelo de produção
try:
    predictor = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/Production")
    USE_MODEL = True
    print(f"✅ Modelo {MODEL_NAME} carregado")
except Exception as e:
    USE_MODEL = False
    print(f"⚠️  Modelo não disponível ({e}). Usando ELO puro.")

FEATURE_COLS = [
    "elo_diff","elo_home_win_prob","elo_home","elo_away",
    "xg_diff","ppda_diff","possession_diff","dominance_diff",
    "home_xg_per_game","home_xga_per_game","home_ppda","home_possession_pct",
    "home_prog_passes","home_dominance_score",
    "away_xg_per_game","away_xga_per_game","away_ppda","away_possession_pct",
    "away_prog_passes","away_dominance_score",
    "home_form_xg","home_form_xga","home_form_ppda","home_form_wins",
    "away_form_xg","away_form_xga","away_form_ppda","away_form_wins",
    "form_xg_diff","form_wins_diff","stage_ordinal",
]

def build_features(home: str, away: str, stage: int = 1) -> np.ndarray:
    """Constrói o vetor de features para uma partida."""
    elo_h = get_elo(home)
    elo_a = get_elo(away)
    elo_diff = elo_h - elo_a
    elo_home_win_prob = 1 / (1 + 10 ** (-elo_diff / 400))

    h_xg   = get_style(home, "overall_xg_per_game", DEFAULT_XG)
    h_xga  = get_style(home, "overall_xga_per_game", DEFAULT_XGA)
    h_ppda = get_style(home, "overall_ppda", DEFAULT_PPDA)
    h_poss = get_style(home, "overall_possession_pct", 50.0)
    h_prog = get_style(home, "overall_prog_passes", 15.0)
    h_dom  = get_style(home, "overall_dominance_score", 0.0)

    a_xg   = get_style(away, "overall_xg_per_game", DEFAULT_XG)
    a_xga  = get_style(away, "overall_xga_per_game", DEFAULT_XGA)
    a_ppda = get_style(away, "overall_ppda", DEFAULT_PPDA)
    a_poss = get_style(away, "overall_possession_pct", 50.0)
    a_prog = get_style(away, "overall_prog_passes", 15.0)
    a_dom  = get_style(away, "overall_dominance_score", 0.0)

    features = [
        elo_diff, elo_home_win_prob, elo_h, elo_a,
        h_xg - a_xg, h_ppda - a_ppda, h_poss - a_poss, h_dom - a_dom,
        h_xg, h_xga, h_ppda, h_poss, h_prog, h_dom,
        a_xg, a_xga, a_ppda, a_poss, a_prog, a_dom,
        # forma (sem histórico Copa 2026 ainda — usar médias gerais)
        h_xg, h_xga, h_ppda, 0.0,
        a_xg, a_xga, a_ppda, 0.0,
        h_xg - a_xg, 0.0,
        stage,
    ]
    return np.array(features).reshape(1, -1)


def predict_match(home: str, away: str, stage: int = 1) -> tuple[float, float, float]:
    """
    Retorna (p_home_win, p_draw, p_away_win).
    Usa o modelo se disponível, senão usa ELO puro.
    """
    if USE_MODEL:
        feats = build_features(home, away, stage)
        probs = predictor.predict_proba(feats)[0]
        # Classes: 0=away win, 1=draw, 2=home win
        return probs[2], probs[1], probs[0]
    else:
        # ELO puro — sem empate em eliminatórias, distribuição uniforme de empate
        elo_h = get_elo(home)
        elo_a = get_elo(away)
        p_hw_raw = 1 / (1 + 10 ** (-(elo_h - elo_a) / 400))
        p_draw   = 0.26 * (1 - abs(p_hw_raw - 0.5) * 1.5)
        p_draw   = max(0.15, min(0.30, p_draw))
        p_hw     = p_hw_raw * (1 - p_draw)
        p_aw     = (1 - p_hw_raw) * (1 - p_draw)
        return p_hw, p_draw, p_aw


def simulate_match(home: str, away: str, stage: int = 1,
                   knockout: bool = False) -> tuple[str, str]:
    """
    Simula um jogo e retorna (vencedor, perdedor).
    Em knockout, empate vai para pênaltis (50/50 extra).
    """
    p_hw, p_draw, p_aw = predict_match(home, away, stage)
    r = np.random.random()
    if r < p_hw:
        return home, away
    elif r < p_hw + p_draw:
        if knockout:
            # Pênaltis: 50/50
            if np.random.random() < 0.5:
                return home, away
            else:
                return away, home
        return None, None   # empate na fase de grupos
    else:
        return away, home

# COMMAND ----------

# MAGIC %md ## 4. Simulação da fase de grupos

# COMMAND ----------

def simulate_groups() -> dict:
    """
    Simula todos os grupos e retorna os classificados.
    Critérios FIFA: pontos → saldo de gols → gols marcados → H2H
    Retorna: {"A": [1º, 2º, 3º, 4º], "B": [...], ...}
    """
    group_results = {}

    for group, teams in WC_2026_GROUPS.items():
        standings = {t: {"pts": 0, "gf": 0, "ga": 0, "gd": 0} for t in teams}
        n = len(teams)

        # Round-robin: cada par joga uma vez
        for i in range(n):
            for j in range(i + 1, n):
                home, away = teams[i], teams[j]
                winner, loser = simulate_match(home, away, stage=1)

                if winner:
                    # Simular placar aproximado baseado nos xG
                    h_xg = get_style(home, "overall_xg_per_game", 1.2)
                    a_xg = get_style(away, "overall_xg_per_game", 1.2)
                    if winner == home:
                        gf, ga = max(1, np.random.poisson(h_xg)), np.random.poisson(a_xg * 0.7)
                    else:
                        gf, ga = np.random.poisson(h_xg * 0.7), max(1, np.random.poisson(a_xg))
                    standings[winner]["pts"] += 3
                    standings[winner]["gf"]  += gf
                    standings[winner]["ga"]  += ga
                    standings[loser]["gf"]   += ga
                    standings[loser]["ga"]   += gf
                else:
                    # Empate
                    goals = max(0, np.random.poisson(1.1))
                    standings[home]["pts"] += 1; standings[home]["gf"] += goals; standings[home]["ga"] += goals
                    standings[away]["pts"] += 1; standings[away]["gf"] += goals; standings[away]["ga"] += goals

        for t in standings:
            standings[t]["gd"] = standings[t]["gf"] - standings[t]["ga"]

        ranked = sorted(
            teams,
            key=lambda t: (standings[t]["pts"], standings[t]["gd"], standings[t]["gf"]),
            reverse=True,
        )
        group_results[group] = ranked

    return group_results


def get_qualifiers(group_results: dict) -> tuple[list, list]:
    """
    Retorna (qualificados_top2, terceiros_classificados).
    Top 2 de cada grupo (24 times) + 8 melhores terceiros = 32 times.
    """
    top2       = []
    thirds     = []
    for grp, ranked in group_results.items():
        top2.extend(ranked[:2])
        if len(ranked) >= 3:
            thirds.append(ranked[2])

    # Sortear 8 melhores terceiros (simplificado: aleatório)
    best_thirds = np.random.choice(thirds, size=min(8, len(thirds)), replace=False).tolist()
    return top2, best_thirds


def simulate_knockout(teams: list, stage: int) -> str:
    """
    Simula mata-mata até restar 1 vencedor.
    Teams deve ter tamanho potência de 2.
    """
    current = teams[:]
    np.random.shuffle(current)

    while len(current) > 1:
        next_round = []
        for i in range(0, len(current), 2):
            if i + 1 < len(current):
                winner, _ = simulate_match(
                    current[i], current[i + 1],
                    stage=stage, knockout=True
                )
                next_round.append(winner)
            else:
                next_round.append(current[i])
        current = next_round
        stage = min(stage + 1, 7)

    return current[0]

# COMMAND ----------

# MAGIC %md ## 5. Monte Carlo — N_SIMULATIONS simulações

# COMMAND ----------

# Contadores por fase
phase_counts = {
    team: {
        "group_stage":      0,
        "round_of_32":      0,
        "round_of_16":      0,
        "quarter_finals":   0,
        "semi_finals":      0,
        "final":            0,
        "champion":         0,
    }
    for team in ALL_TEAMS
}

print(f"🎲 Iniciando {N_SIMULATIONS:,} simulações da Copa 2026...")

for sim in range(N_SIMULATIONS):
    if sim % 1000 == 0:
        print(f"  Simulação {sim:,}/{N_SIMULATIONS:,}...")

    # ── Fase de grupos
    group_results = simulate_groups()

    for grp, ranked in group_results.items():
        for i, team in enumerate(ranked):
            if i >= 2:
                phase_counts[team]["group_stage"] += 1

    # ── Qualificados para oitavas (32 times)
    top2, best_thirds = get_qualifiers(group_results)
    r32_teams = top2 + best_thirds
    np.random.shuffle(r32_teams)

    # Todos que chegaram ao mata-mata
    for t in r32_teams:
        phase_counts[t]["round_of_32"] += 1

    # ── Oitavas (32 → 16)
    r16_teams = []
    for i in range(0, len(r32_teams), 2):
        if i + 1 < len(r32_teams):
            w, _ = simulate_match(r32_teams[i], r32_teams[i+1], stage=3, knockout=True)
            r16_teams.append(w)

    for t in r16_teams:
        phase_counts[t]["round_of_16"] += 1

    # ── Quartas (16 → 8)
    np.random.shuffle(r16_teams)
    qf_teams = []
    for i in range(0, len(r16_teams), 2):
        if i + 1 < len(r16_teams):
            w, _ = simulate_match(r16_teams[i], r16_teams[i+1], stage=4, knockout=True)
            qf_teams.append(w)

    for t in qf_teams:
        phase_counts[t]["quarter_finals"] += 1

    # ── Semis (8 → 4)
    np.random.shuffle(qf_teams)
    sf_teams = []
    for i in range(0, len(qf_teams), 2):
        if i + 1 < len(qf_teams):
            w, _ = simulate_match(qf_teams[i], qf_teams[i+1], stage=5, knockout=True)
            sf_teams.append(w)

    for t in sf_teams:
        phase_counts[t]["semi_finals"] += 1

    # ── Final (4 → 2 → 1)
    np.random.shuffle(sf_teams)
    finalists = []
    for i in range(0, len(sf_teams), 2):
        if i + 1 < len(sf_teams):
            w, _ = simulate_match(sf_teams[i], sf_teams[i+1], stage=6, knockout=True)
            finalists.append(w)

    for t in finalists:
        phase_counts[t]["final"] += 1

    # ── Final
    if len(finalists) >= 2:
        champion, _ = simulate_match(finalists[0], finalists[1], stage=7, knockout=True)
        phase_counts[champion]["champion"] += 1

print(f"✅ {N_SIMULATIONS:,} simulações concluídas!")

# COMMAND ----------

# MAGIC %md ## 6. Converter para probabilidades e salvar

# COMMAND ----------

results = []
for team, counts in phase_counts.items():
    results.append({
        "team_name":            team,
        "elo_rating":           get_elo(team),
        "p_group_stage":        round(counts["group_stage"]      / N_SIMULATIONS * 100, 2),
        "p_round_of_32":        round(counts["round_of_32"]      / N_SIMULATIONS * 100, 2),
        "p_round_of_16":        round(counts["round_of_16"]      / N_SIMULATIONS * 100, 2),
        "p_quarter_finals":     round(counts["quarter_finals"]   / N_SIMULATIONS * 100, 2),
        "p_semi_finals":        round(counts["semi_finals"]      / N_SIMULATIONS * 100, 2),
        "p_final":              round(counts["final"]            / N_SIMULATIONS * 100, 2),
        "p_champion":           round(counts["champion"]         / N_SIMULATIONS * 100, 2),
        "n_simulations":        N_SIMULATIONS,
        "group":                next((g for g, ts in WC_2026_GROUPS.items() if team in ts), "?"),
    })

results_df = pd.DataFrame(results).sort_values("p_champion", ascending=False)

print("\n🏆 Probabilidades de ser campeão — Copa 2026:")
print(results_df[["team_name","group","elo_rating",
                   "p_round_of_16","p_quarter_finals",
                   "p_semi_finals","p_final","p_champion"]]
      .head(20).to_string(index=False))

# Salvar no MLflow
mlflow.set_experiment("worldcup/tournament_simulator")
with mlflow.start_run(run_name=f"monte_carlo_{N_SIMULATIONS}"):
    mlflow.log_param("n_simulations", N_SIMULATIONS)
    mlflow.log_param("n_teams", len(ALL_TEAMS))
    mlflow.log_param("format", "48_teams_12_groups")
    mlflow.log_param("use_model", USE_MODEL)
    # Top 5 favoritos
    for i, row in results_df.head(5).iterrows():
        mlflow.log_metric(f"p_champion_{row['team_name'].replace(' ','_')}", row["p_champion"])
    mlflow.log_dict(results_df.to_dict(orient="records"), "simulation_results.json")

# COMMAND ----------

# MAGIC %md ## 7. Salvar na Gold Delta Table

# COMMAND ----------

sim_spark = spark.createDataFrame(results_df) \
    .withColumn("_processed_at", F.current_timestamp()) \
    .withColumn("_model_used", F.lit(MODEL_NAME if USE_MODEL else "ELO_pure"))

sim_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable(f"{CATALOG}.gold.simulation_results")

print(f"\n✅ worldcup.gold.simulation_results: {sim_spark.count()} times")

# COMMAND ----------

# MAGIC %md ## 8. Análises finais

# COMMAND ----------

sims = spark.table(f"{CATALOG}.gold.simulation_results")

print("🌎 Top 20 favoritos ao título:")
sims.select(
    "team_name","group","elo_rating",
    "p_semi_finals","p_final","p_champion"
).orderBy("p_champion", ascending=False).show(20, truncate=False)

print("\n🇧🇷 Brasil — probabilidades detalhadas:")
sims.filter(F.col("team_name") == "Brazil") \
    .select("team_name","elo_rating","group",
            "p_round_of_16","p_quarter_finals",
            "p_semi_finals","p_final","p_champion") \
    .show(truncate=False)

print(f"\n📊 Distribuição de campeões por confederação:")
sims.withColumn("confederation",
    F.when(F.col("team_name").isin(
        ["Brazil","Argentina","Uruguay","Colombia","Ecuador","Chile","Peru","Bolivia","Venezuela","Paraguay"]), "CONMEBOL")
    .when(F.col("team_name").isin(
        ["France","England","Spain","Germany","Portugal","Netherlands","Belgium",
         "Italy","Croatia","Denmark","Switzerland","Poland","Serbia","Austria","Czech Republic","Romania"]), "UEFA")
    .when(F.col("team_name").isin(["USA","Mexico","Canada","Honduras","Panama"]), "CONCACAF")
    .when(F.col("team_name").isin(
        ["Morocco","Senegal","Nigeria","Cameroon","Egypt","Ghana","Algeria","Ivory Coast"]), "CAF")
    .when(F.col("team_name").isin(
        ["Japan","South Korea","Saudi Arabia","Iran","Australia","Iraq","Jordan","Qatar"]), "AFC")
    .otherwise("OTHER")
).groupBy("confederation") \
 .agg(F.round(F.sum("p_champion"), 2).alias("total_p_champion")) \
 .orderBy("total_p_champion", ascending=False) \
 .show(truncate=False)
