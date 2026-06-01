# Fase 04 — Gold

**Status:** ✅ Completa  
**Tabelas geradas:** 6  
**Executar após:** Fase 03 — Silver completa

---

## Ordem de execução

| Ordem | Notebook | Tabelas Gold | Depende de |
|---|---|---|---|
| 1 | `01_gold_top_scorers.py` | `top_scorers` + `top_scorers_cross_tournament` | silver.player_match_stats |
| 2 | `02_gold_team_style.py` | `team_style` + `team_style_overall` | silver.team_performance |
| 3 | `03_gold_match_features.py` | `match_features` | silver.team_performance + silver.elo_* + gold.team_style_overall + bronze.statsbomb_matches |
| 4 | `04_gold_standings.py` | `standings` | silver.team_performance + bronze.statsbomb_matches |

---

## O que cada tabela entrega

### top_scorers
- Artilharia por torneio: gols, xG, gols-xG, assistências, xA, chutes, conversão %
- Ranking dentro de cada competição (`rank_in_competition`)
- Métricas por 90min: `goals_per_90`, `xg_per_90`

### top_scorers_cross_tournament
- Ranking consolidado dos últimos 4 torneios (WC 2018 + WC 2022 + Euro 2020 + Euro 2024)
- `goal_contributions` = gols + assistências (métrica de impacto total)
- Lista de competições em que cada jogador apareceu

### team_style
- Perfil tático automático: `PRESSING_ALTO`, `POSSE_DOMINANTE`, `CONTRA_ATAQUE`, `BLOCO_BAIXO`, `EQUILIBRADO`
- Baseado em: PPDA, posse %, progressive_passes, defensive_line_x
- `dominance_score` = score composto (xG_diff + posse + pressing) — feature ML

### team_style_overall
- Médias de todos os torneios combinados por seleção
- `overall_dominance_score` — feature #3 mais importante no match predictor

### match_features (Feature Store)
- **1 linha por partida — 45+ features lado a lado (home vs away)**
- ELO: `elo_home`, `elo_away`, `elo_diff`, `elo_home_win_prob`
- Estilo: xG/jogo, xGA/jogo, PPDA, posse %, passes progressivos
- Diferenciais sintéticos: `xg_diff`, `ppda_diff`, `dominance_diff`
- Forma recente: `home_form_xg`, `home_form_wins` (últimas 5 partidas)
- **Target `result_home`**: 1=vitória home, 0=empate, -1=vitória away
- Esta é a tabela que o notebook ML lê diretamente

### standings
- Classificação geral por torneio com ranking
- `luck_index` = pontos reais − pontos esperados por xG (identifica times sortudos/azarados)
- `furthest_stage` = fase máxima atingida pelo time no torneio

---

## Hierarquia completa de tabelas (Bronze → Silver → Gold)

```
Bronze (raw)          Silver (clean)           Gold (serving)
──────────────────    ─────────────────────    ──────────────────────
statsbomb_matches  →  match_timeline       →   match_features (ML)
statsbomb_events   →  player_match_stats   →   top_scorers
statsbomb_lineups  →  team_performance     →   team_style
statsbomb_360      →  shot_map             →   standings
fbref_player_season   elo_ratings          →   top_scorers_cross_tournament
historical_results    elo_current_snapshot     team_style_overall
```

---

## Validações de negócio esperadas

```python
# Copa 2022 — Argentina campeã com 7 jogos, 12 pontos na fase final
spark.table("worldcup.gold.standings") \
    .filter(
        (col("_competition_label") == "FIFA World Cup 2022") &
        (col("team_name") == "Argentina")
    ).show()

# Mbappé artilheiro WC 2022 com 8 gols
spark.table("worldcup.gold.top_scorers") \
    .filter(
        (col("_competition_label") == "FIFA World Cup 2022") &
        (col("rank_in_competition") == 1)
    ).show()

# match_features deve ter 262 linhas (todas as partidas StatsBomb)
spark.table("worldcup.gold.match_features").count()  # → 262
```

---

## Próxima Fase

**Fase 05 — ML**: xG model, match predictor (XGBoost), Monte Carlo simulator.
Todos os notebooks ML leem de `worldcup.gold.match_features` e `worldcup.silver.shot_map`.

Arquivo: `docs/PHASE_05_ML.md`
