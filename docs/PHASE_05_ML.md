# Fase 05 — Machine Learning

**Status:** ✅ Completa  
**Notebooks:** 3  
**Modelos MLflow:** 2 registrados em Production  
**Tabelas Gold geradas:** 3  
**Executar após:** Fase 04 — Gold completa

---

## Ordem de execução

| Ordem | Notebook | Modelo MLflow | Tabela gerada |
|---|---|---|---|
| 1 | `01_ml_xg_model.py` | `worldcup-xg-model` → Production | `worldcup.gold.xg_predictions` |
| 2 | `02_ml_match_predictor.py` | `worldcup-match-predictor` → Production | `worldcup.gold.match_predictions` |
| 3 | `03_ml_tournament_simulator.py` | — (usa modelo 2) | `worldcup.gold.simulation_results` |

---

## Notebook 01 — xG Model

**Objetivo:** Treinar modelo próprio de Expected Goals usando dados reais StatsBomb.

**Features:**
- Geométricas: `distance_to_goal`, `angle_to_goal`
- Técnicas: `shot_body_part`, `shot_technique`, `shot_type`, `shot_first_time`, `shot_one_on_one`
- Contexto: `under_pressure`, `period`, `score_diff_at_event`, `pitch_zone`
- 360° (quando disponível): `defenders_in_cone`, `distance_nearest_defender`, `gk_distance_to_goalline`

**Split:** WC 2018 + Euro 2020 + Copa América 2024 → treino | WC 2022 + Euro 2024 → teste

**Algoritmos comparados:**
- Logistic Regression (baseline com StandardScaler)
- XGBoost (modelo principal, com features 360°)

**Métricas esperadas:**
- ROC-AUC: ~0.78-0.82 (StatsBomb xG baseline: ~0.80)
- Brier Score: ~0.09
- Log Loss: ~0.30

**Output:** Coluna `our_xg` para cada chute — comparável ao `shot_xg` do StatsBomb.

---

## Notebook 02 — Match Predictor

**Objetivo:** Prever resultado de partida (W/D/L) com probabilidades calibradas.

**Features:** 31 features do `worldcup.gold.match_features`:
- ELO: `elo_diff`, `elo_home_win_prob` (features mais importantes ~38%)
- Estilo: `xg_diff`, `ppda_diff`, `dominance_diff` (~30%)
- Forma recente: `form_xg_diff`, `home_form_wins` (~20%)
- Contexto: `stage_ordinal` (~12%)

**Validação:** Leave-One-Tournament-Out (LOTO)
- Treina em 4 torneios, testa no 5º — repete para cada torneio
- Mais realista que random split para dados temporais

**Algoritmos:** XGBoost + LightGBM (o melhor vai para Production)

**Métricas esperadas:**
- LOTO accuracy: ~55-62% (baseline "sempre home win": ~46%)
- O modelo bate o baseline em todos os torneios

**Classes:**
- `0` = away win
- `1` = draw
- `2` = home win

**Output:** `p_home_win`, `p_draw`, `p_away_win` para cada partida histórica.

---

## Notebook 03 — Tournament Simulator

**Objetivo:** Simular a Copa 2026 10.000 vezes e calcular probabilidades de cada seleção.

**Metodologia Monte Carlo:**
1. Usa o match predictor para obter P(W), P(D), P(L) de cada jogo
2. Sorteia resultado com `np.random.random()` contra as probabilidades
3. Simula fase de grupos (round-robin, critérios FIFA)
4. Classifica top 2 de cada grupo + 8 melhores terceiros = 32 times
5. Simula mata-mata (oitavas → quartas → semis → final)
6. Pênaltis = 50/50 em empate no mata-mata
7. Repete 10.000 vezes

**Formato Copa 2026:** 48 times, 12 grupos de 4, 104 jogos

**Output por seleção:**
- `p_round_of_16`, `p_quarter_finals`, `p_semi_finals`, `p_final`, `p_champion`

**Fallback:** Se o modelo não estiver disponível, usa ELO puro (fórmula Elo World Football).

---

## Como rodar no Databricks

```python
# Instalar dependências no cluster (rodar em célula separada)
%pip install xgboost lightgbm

# Verificar modelos registrados
import mlflow
client = mlflow.tracking.MlflowClient()

for model_name in ["worldcup-xg-model", "worldcup-match-predictor"]:
    versions = client.get_latest_versions(model_name, stages=["Production"])
    for v in versions:
        print(f"{model_name} v{v.version} → {v.current_stage}")
```

---

## Validações de negócio esperadas

```python
from pyspark.sql import functions as F

# xG model — taxa de conversão deve ser ~10% (gols/chutes)
spark.table("worldcup.gold.xg_predictions") \
    .agg(
        F.avg("our_xg").alias("avg_xg"),
        F.avg(F.col("is_goal").cast("int")).alias("taxa_real"),
        F.corr("our_xg", "shot_xg").alias("corr_statsbomb"),
    ).show()

# Match predictor — distribuição de resultados
spark.table("worldcup.gold.match_predictions") \
    .groupBy("_competition_label") \
    .agg(
        F.avg("correct").alias("accuracy"),
        F.avg("p_home_win").alias("avg_p_home"),
        F.avg("p_draw").alias("avg_p_draw"),
    ).show()

# Simulador — Brasil deve ter ~15-25% de chance de chegar na semi
spark.table("worldcup.gold.simulation_results") \
    .filter(F.col("team_name") == "Brazil") \
    .select("p_semi_finals","p_final","p_champion") \
    .show()

# Top 5 favoritos ao título
spark.table("worldcup.gold.simulation_results") \
    .orderBy("p_champion", ascending=False) \
    .select("team_name","elo_rating","p_semi_finals","p_final","p_champion") \
    .show(5)
```

---

## Tabelas Gold geradas nesta fase

| Tabela | Registros | Descrição |
|---|---|---|
| `worldcup.gold.xg_predictions` | ~15.000 | xG do nosso modelo para cada chute |
| `worldcup.gold.match_predictions` | ~262 | P(W/D/L) para cada partida histórica |
| `worldcup.gold.simulation_results` | 48 | % de cada seleção em cada fase da Copa 2026 |

---

## Hierarquia completa de tabelas — projeto finalizado

```
INGESTÃO (local)
  01_statsbomb_downloader.py  →  data/raw/statsbomb/
  02_fbref_scraper.py         →  data/raw/fbref/
  03_openfootball_loader.py   →  data/raw/historical/
        ↓ upload DBFS
BRONZE
  statsbomb_matches           →  262 partidas reais
  statsbomb_events            →  ~890.000 eventos
  statsbomb_lineups           →  ~5.750 jogadores x partida
  statsbomb_360               →  ~15M freeze frames
  fbref_player_season         →  ~10.000 registros
  historical_results          →  ~870 partidas (1930-2022)
        ↓
SILVER
  match_timeline              →  ~600.000 eventos limpos + features
  player_match_stats          →  ~5.500 jogadores x partida
  team_performance            →  ~524 times x partida
  shot_map                    →  ~15.000 chutes com contexto 360°
  elo_ratings                 →  ~870 partidas com ELO calculado
  elo_current_snapshot        →  ELO atual de cada seleção
        ↓
GOLD
  top_scorers                 →  Artilharia por torneio
  top_scorers_cross_tournament
  team_style                  →  Perfil tático por torneio
  team_style_overall          →  Perfil tático consolidado
  match_features              →  Feature store (45+ features por partida)
  standings                   →  Classificação com luck_index
        ↓
ML (MLflow)
  worldcup-xg-model           →  xG por chute (XGBoost)
  worldcup-match-predictor    →  P(W/D/L) por partida (XGBoost/LGBM)
        ↓
GOLD (inferência)
  xg_predictions              →  our_xg para ~15k chutes
  match_predictions           →  probabilidades para 262 partidas
  simulation_results          →  Monte Carlo Copa 2026 (48 times)
```
