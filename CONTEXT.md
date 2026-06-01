# WorldCup Data Platform — Master Context (v2)

> Documento MCP. Toda sessão começa lendo este arquivo.
> Atualizar ao fim de cada fase.

---

## Stack & Decisões Fundamentais

| Camada | Tecnologia |
|---|---|
| Processamento | Databricks Community Edition |
| Storage | Delta Lake (`dbfs:/FileStore/wc-platform/`) |
| Catálogo | Unity Catalog → `worldcup` (fallback: `hive_metastore`) |
| Orquestração | Manual (CE não suporta Jobs API) — DAB como documentação |
| ML | MLflow embutido no Databricks |
| Linguagem | Python 3.10+ / PySpark |

### Decisões fixas
- ❌ Sem Kafka / streaming (removido — batch puro)
- ❌ Sem dados simulados — **100% dados reais**
- ✅ StatsBombPy como fonte primária de eventos
- ✅ FBref via `soccerdata` para stats de clube (temporada atual)
- ✅ Scraping respeitoso: delays, User-Agent, sem burlar robots.txt

---

## Fontes de Dados Reais

| Fonte | O que dá | IDs |
|---|---|---|
| **StatsBomb Open Data** | Eventos partida (passes, chutes, pressão, xG, 360°), lineups | Ver tabela abaixo |
| **FBref** (`soccerdata`) | Stats de jogador por temporada (gols, xG, assistências, pressão, posse) | top-5 ligas + seleções |
| **football-data.org** | Fixtures, standings, resultados Copa 2026 | API key free |
| **OpenMeteo** | Clima real das sedes | Sem auth |
| **openfootball/worldcup** | Resultados históricos 1930-2022 | GitHub JSON |

### StatsBomb — Competitions mapeadas

| Competição | competition_id | season_id | Jogos | 360? |
|---|---|---|---|---|
| FIFA World Cup 2022 | 43 | 106 | 64 | ✅ |
| FIFA World Cup 2018 | 43 | 3 | 64 | ❌ |
| UEFA Euro 2024 | 55 | 282 | 51 | ✅ |
| Copa América 2024 | 223 | 282 | 32 | ❌ |
| UEFA Euro 2020 | 55 | 43 | 51 | ✅ |

**Total: ~262 partidas reais de seleções com eventos completos**

---

## Estrutura de Diretórios

```
wc-data-platform/
├── CONTEXT.md                        ← este arquivo
├── README.md
├── requirements.txt
│
├── ingestion/                        ← scripts de coleta (rodam local)
│   ├── 01_statsbomb_downloader.py    ← baixa todos os JSONs do GitHub
│   ├── 02_fbref_scraper.py           ← stats de jogadores temporada atual
│   ├── 03_openfootball_loader.py     ← resultados históricos 1930-2022
│   └── config.py
│
├── notebooks/                        ← notebooks Databricks (.py)
│   ├── bronze/
│   │   ├── 01_bronze_statsbomb_events.py
│   │   ├── 02_bronze_statsbomb_lineups.py
│   │   ├── 03_bronze_statsbomb_360.py
│   │   ├── 04_bronze_fbref_players.py
│   │   └── 05_bronze_historical_results.py
│   ├── silver/
│   │   ├── 01_silver_match_timeline.py
│   │   ├── 02_silver_player_stats.py
│   │   ├── 03_silver_team_performance.py
│   │   └── 04_silver_shot_map.py
│   ├── gold/
│   │   ├── 01_gold_standings.py
│   │   ├── 02_gold_top_scorers.py
│   │   ├── 03_gold_match_features.py
│   │   └── 04_gold_team_style.py
│   └── ml/
│       ├── 01_xg_model.py
│       ├── 02_match_predictor.py
│       └── 03_tournament_simulator.py
│
├── bundle/
│   ├── databricks.yml
│   └── resources/jobs.yml
│
└── docs/
    ├── PHASE_01_INGESTION.md
    ├── PHASE_02_BRONZE.md
    └── ...
```

---

## Unity Catalog — Tabelas

```
worldcup
├── bronze
│   ├── statsbomb_events      (Delta, particionado por competition_id, season_id)
│   ├── statsbomb_lineups     (Delta, particionado por match_id)
│   ├── statsbomb_360         (Delta, particionado por match_id)
│   ├── fbref_player_season   (Delta, particionado por season, league)
│   └── historical_results    (Delta, particionado por year)
├── silver
│   ├── match_timeline        (Delta MERGE por event_id)
│   ├── player_match_stats    (Delta MERGE por player_id + match_id)
│   ├── team_performance      (Delta MERGE por team_id + match_id)
│   └── shot_map              (Delta MERGE por event_id — só chutes com xG + coordenadas)
└── gold
    ├── top_scorers           (xG acumulado + gols reais por torneio)
    ├── team_style_index      (pressing, posse, PPDA, profundidade)
    ├── match_features        (feature store para ML)
    └── simulation_results    (Monte Carlo — probabilidades Copa 2026)
```

---

## Estado do Projeto

| Fase | Status |
|---|---|
| 01 — Ingestion | ✅ Completa |
| 02 — Bronze | ✅ Completa |
| 03 — Silver | ✅ Completa |
| 04 — Gold | ⏳ |
| 05 — ML | ⏳ |

---

## Log de Decisões

### 2026-05 — Redesign v2
- Removido Kafka/streaming completamente — batch puro
- Removido todos os dados simulados
- StatsBombPy como fonte primária (262 partidas reais de seleções)
- FBref via `soccerdata` para temporada 2024-25 dos clubes
- openfootball para histórico 1930-2022
