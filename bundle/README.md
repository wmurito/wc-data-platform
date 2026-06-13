# Bundle WC Data Platform 2026 — Referência Rápida

> ⚠️ **Community Edition**: Jobs API não suportada. Use este bundle em workspace pago.
> Para CE, execute os notebooks manualmente na ordem documentada abaixo.

---

## Estrutura do Bundle

```
bundle/
├── databricks.yml              ← Root config (workspace, targets, includes)
└── resources/
    ├── 02_bronze_job.yml       ← Fase Bronze  (6 tasks, ~15-20 min)
    ├── 03_silver_job.yml       ← Fase Silver  (6 tasks, ~20-25 min)
    ├── 04_gold_job.yml         ← Fase Gold    (5 tasks, ~10-15 min)
    ├── 05_ml_job.yml           ← Fase ML      (3 tasks, ~20-30 min, agendado 06:00 UTC)
    ├── 06_serving_job.yml      ← Fase Serving (2 tasks, ~5 min)
    └── 07_full_pipeline_job.yml← Pipeline completo end-to-end
```

---

## Ordem de Execução

```
BRONZE ──────────────────────────────────────────────────────────────
  01_statsbomb_matches   (base referencial)
  ├── 02_statsbomb_events    (depende de matches)
  ├── 03_statsbomb_lineups   (depende de matches, paralelo com events)
  │   └── 04_statsbomb_360   (depende de events + lineups)
  ├── 05_fbref_players       (depende de matches, paralelo)
  └── 06_historical_results  (depende de matches, paralelo)

SILVER ──────────────────────────────────────────────────────────────
  01_match_timeline      (depende de bronze: events + lineups + 360)
  ├── 02_player_stats        (depende de match_timeline)
  │   ├── 03_team_performance    (depende de player_stats)
  │   │   └── 05_elo_ratings     (depende de team_performance + historical)
  │   └── 06_squad_features      (depende de player_stats + fbref)
  └── 04_shot_map            (depende de match_timeline, paralelo)

GOLD ────────────────────────────────────────────────────────────────
  ├── 01_top_scorers     (depende de silver: player_stats + shot_map)
  ├── 02_team_style      (depende de silver: team_performance + squad_features)
  │   ├── 03_match_features  (depende de top_scorers + team_style + elo)
  │   │   └── 03b_match_features_v2  (depende de match_features)
  │   └── 04_standings       (depende de team_style)

ML ──────────────────────────────────────────────────────────────────
  01_xg_model            (depende de gold: match_features + v2)
  └── 02_match_predictor     (depende de xg_model + standings)
      └── 03_tournament_simulator (depende de match_predictor)

SERVING ─────────────────────────────────────────────────────────────
  01_export_gold_to_csv  (depende de ML completo + gold standings/scorers)
  └── 02_fabric_lakehouse_setup (depende de export)
```

---

## Comandos

```bash
# Configurar variáveis de ambiente
export DATABRICKS_HOST="https://<workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export TF_VAR_ALERT_EMAIL="seu@email.com"

# Validar bundle (sem deploy)
databricks bundle validate --target dev

# Deploy para dev
databricks bundle deploy --target dev

# Executar apenas Bronze
databricks bundle run wc_bronze_pipeline --target dev

# Executar apenas Silver
databricks bundle run wc_silver_pipeline --target dev

# Executar apenas Gold
databricks bundle run wc_gold_pipeline --target dev

# Executar apenas ML
databricks bundle run wc_ml_pipeline --target dev

# Executar apenas Serving
databricks bundle run wc_serving_pipeline --target dev

# Executar pipeline COMPLETO end-to-end
databricks bundle run wc_full_pipeline --target dev

# Deploy para produção
databricks bundle deploy --target prod
```

---

## Variáveis Necessárias

| Variável env | Descrição |
|---|---|
| `DATABRICKS_HOST` | URL do workspace (ex: `https://adb-123.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Personal Access Token ou Service Principal token |
| `ALERT_EMAIL` | E-mail para notificações de falha/sucesso |
| `SP_CLIENT_ID` | Service Principal ID (apenas para target `prod`) |

---

## Jobs Criados

| Job ID | Nome | Fases | Tempo estimado |
|---|---|---|---|
| `wc_bronze_pipeline` | WC 2026 · 02 · Bronze | 6 tasks | 15-20 min |
| `wc_silver_pipeline` | WC 2026 · 03 · Silver | 6 tasks | 20-25 min |
| `wc_gold_pipeline`   | WC 2026 · 04 · Gold   | 5 tasks | 10-15 min |
| `wc_ml_pipeline`     | WC 2026 · 05 · ML     | 3 tasks | 20-30 min |
| `wc_serving_pipeline`| WC 2026 · 06 · Serving| 2 tasks | 5 min |
| `wc_full_pipeline`   | WC 2026 · FULL        | 22 tasks total | ~90 min |
