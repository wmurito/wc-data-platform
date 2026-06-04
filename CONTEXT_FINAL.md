# WorldCup Data Platform — Master Context (v3 — FINAL)

> Documento MCP. Toda sessão começa lendo este arquivo.

---

## Stack & Decisões Fundamentais

| Camada | Tecnologia |
|---|---|
| Processamento | Databricks Community Edition |
| Storage | Delta Lake (`dbfs:/FileStore/wc-platform/`) |
| Catálogo | Unity Catalog → `worldcup` (fallback: `hive_metastore`) |
| Orquestração | Manual (CE não suporta Jobs API) |
| ML | MLflow embutido no Databricks |
| Linguagem | Python 3.10+ / PySpark |

### Decisões fixas
- ❌ Sem Kafka / streaming — batch puro
- ❌ Sem dados simulados — 100% dados reais
- ✅ StatsBombPy como fonte primária de eventos
- ✅ FBref via `soccerdata` para stats de clube
- ✅ openfootball para histórico 1930-2022

---

## Fontes de Dados

| Fonte | Dados | Acesso |
|---|---|---|
| StatsBomb Open Data | Eventos, lineups, 360° — 262 partidas reais | GitHub, gratuito |
| FBref (soccerdata) | Stats jogadores top-5 ligas 2024-25 | Scraping, gratuito |
| openfootball/worldcup | Resultados 1930-2022 | GitHub JSON, gratuito |
| API-Football (opcional) | Live data Copa 2026 | RapidAPI — free: 100 req/dia |

### StatsBomb — competitions

| Competição | competition_id | season_id | Jogos | 360°? |
|---|---|---|---|---|
| FIFA World Cup 2022 | 43 | 106 | 64 | ✅ |
| FIFA World Cup 2018 | 43 | 3 | 64 | ❌ |
| UEFA Euro 2024 | 55 | 282 | 51 | ✅ |
| UEFA Euro 2020 | 55 | 43 | 51 | ✅ |
| Copa América 2024 | 223 | 282 | 32 | ❌ |

---

## Estado do Projeto — COMPLETO ✅

| Fase | Status | Notebooks/Scripts |
|---|---|---|
| 01 — Ingestion | ✅ Completa | 3 scripts Python |
| 02 — Bronze | ✅ Completa | 6 notebooks |
| 03 — Silver | ✅ Completa | 5 notebooks |
| 04 — Gold | ✅ Completa | 4 notebooks |
| 05 — ML | ✅ Completa | 3 notebooks |

**Total: 21 arquivos de código**

---

## Todas as Tabelas Delta

```
worldcup.bronze
  statsbomb_matches       (~262 linhas)
  statsbomb_events        (~890k linhas)
  statsbomb_lineups       (~5.750 linhas)
  statsbomb_360           (~15M linhas)
  fbref_player_season     (~10k linhas)
  historical_results      (~870 linhas)

worldcup.silver
  match_timeline          (~600k linhas)
  player_match_stats      (~5.5k linhas)
  team_performance        (~524 linhas)
  shot_map                (~15k linhas)
  elo_ratings             (~870 linhas)
  elo_current_snapshot    (1 linha por seleção)

worldcup.gold
  top_scorers             (por torneio)
  top_scorers_cross_tournament
  team_style              (por torneio)
  team_style_overall
  match_features          (feature store — 45+ cols)
  standings               (com luck_index)
  xg_predictions          (~15k chutes com our_xg)
  match_predictions       (~262 partidas)
  simulation_results      (48 seleções Copa 2026)

MLflow Model Registry
  worldcup-xg-model       → Production
  worldcup-match-predictor → Production
```

---

## Modelos MLflow

| Modelo | Algoritmo | Target | Métrica esperada |
|---|---|---|---|
| `worldcup-xg-model` | XGBoost | is_goal (0/1) | ROC-AUC ~0.80 |
| `worldcup-match-predictor` | XGBoost/LGBM | result_home (W/D/L) | LOTO acc ~58% |
