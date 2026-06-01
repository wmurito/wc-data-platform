# Fase 02 — Bronze

**Status:** ✅ Completa  
**Tabelas geradas:** 6  
**Executar após:** Fase 01 (dados no DBFS)

---

## Ordem de execução dos notebooks

| Ordem | Notebook | Tabela Bronze | Depende de |
|---|---|---|---|
| 1 | `01_bronze_statsbomb_matches.py` | `worldcup.bronze.statsbomb_matches` | matches.json no DBFS |
| 2 | `02_bronze_statsbomb_events.py` | `worldcup.bronze.statsbomb_events` | events/*.json no DBFS |
| 3 | `03_bronze_statsbomb_lineups.py` | `worldcup.bronze.statsbomb_lineups` | lineups/*.json no DBFS |
| 4 | `04_bronze_statsbomb_360.py` | `worldcup.bronze.statsbomb_360` | three-sixty/*.json no DBFS |
| 5 | `05_bronze_fbref_players.py` | `worldcup.bronze.fbref_player_season` | *.parquet no DBFS |
| 6 | `06_bronze_historical_results.py` | `worldcup.bronze.historical_results` | wc_*.json no DBFS |

---

## Volumes esperados

| Tabela | Registros estimados | Partição |
|---|---|---|
| statsbomb_matches | ~262 | _competition_id |
| statsbomb_events | ~890.000 (262 x 3.400) | _competition_id, period |
| statsbomb_lineups | ~5.750 (262 x 22) | _competition_id |
| statsbomb_360 | ~15M (176 jogos x ~85.000 frames) | _competition_id |
| fbref_player_season | ~2.000 por liga x 5 ligas x 3 stats | stat_category |
| historical_results | ~870 partidas (1930-2022) | year |

---

## Princípios desta camada

1. **Preservação total** — nenhum campo descartado na Bronze. A Silver filtra.
2. **Append-only** para events/lineups/360 — overwrite só em statsbomb_matches e historical_results (referência completa).
3. **mergeSchema=true** — suporta variações de schema entre competições.
4. **Fallback Unity Catalog** — cada notebook tenta UC primeiro, cai em hive_metastore se não disponível.
5. **_ingested_at** em todas as tabelas para rastreabilidade.

---

## Como importar notebooks no Databricks

1. Abrir Databricks Community Edition
2. Workspace → Import → selecionar arquivo `.py`
3. Ou: colar o conteúdo em um novo notebook (File → New Notebook → Python)

---

## Próxima Fase

**Fase 03 — Silver**: Normalização, MERGE upsert, enriquecimento cruzado entre tabelas, cálculo de ELO, feature engineering.

Arquivo: `docs/PHASE_03_SILVER.md`
