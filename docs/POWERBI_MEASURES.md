# Power BI — Medidas DAX para o Dashboard

Copie estas medidas no Semantic Model após conectar as tabelas.

---

## Medidas — Artilharia

```dax
Total Gols =
SUM(gold_top_scorers[goals])

Total xG =
ROUND(SUM(gold_top_scorers[xg_total]), 2)

Gols Acima xG =
ROUND([Total Gols] - [Total xG], 2)

Taxa Conversão % =
DIVIDE(
    SUM(gold_top_scorers[goals]),
    SUM(gold_top_scorers[shots_total]),
    0
) * 100

Artilheiros com Gols =
CALCULATE(
    DISTINCTCOUNT(gold_top_scorers[player_name]),
    gold_top_scorers[goals] >= 1
)
```

---

## Medidas — Simulação Copa 2026

```dax
% Favorito Título =
CALCULATE(
    MAX(gold_simulation_results[p_champion]),
    TOPN(1, gold_simulation_results, gold_simulation_results[p_champion], DESC)
)

Favorito Nome =
CALCULATE(
    FIRSTNONBLANK(gold_simulation_results[team_name], 1),
    TOPN(1, gold_simulation_results, gold_simulation_results[p_champion], DESC)
)

Brasil % Título =
CALCULATE(
    MAX(gold_simulation_results[p_champion]),
    gold_simulation_results[team_name] = "Brazil"
)

Brasil % Semi =
CALCULATE(
    MAX(gold_simulation_results[p_semi_finals]),
    gold_simulation_results[team_name] = "Brazil"
)

CONMEBOL % Título =
CALCULATE(
    SUM(gold_simulation_results[p_champion]),
    gold_simulation_results[team_name] IN {
        "Brazil","Argentina","Uruguay","Colombia","Ecuador","Chile"
    }
)
```

---

## Medidas — xG Model

```dax
Total Chutes =
COUNT(gold_xg_predictions[event_id])

xG Médio por Chute =
AVERAGE(gold_xg_predictions[our_xg])

Precisão xG vs StatsBomb =
1 - AVERAGE(ABS(gold_xg_predictions[our_xg] - gold_xg_predictions[shot_xg]))

Taxa de Gol Real =
DIVIDE(
    CALCULATE(COUNT(gold_xg_predictions[event_id]),
              gold_xg_predictions[is_goal] = TRUE()),
    COUNT(gold_xg_predictions[event_id])
)
```

---

## Medidas — Estilos Táticos

```dax
PPDA Médio =
AVERAGE(gold_team_style[avg_ppda])

Times Pressing Alto =
CALCULATE(
    DISTINCTCOUNT(gold_team_style[team_name]),
    gold_team_style[tactical_profile] = "PRESSING_ALTO"
)

xG Diferencial Médio =
AVERAGE(gold_team_style[xg_diff_per_game])

Melhor xG Diferencial =
CALCULATE(
    MAX(gold_team_style[xg_diff_per_game]),
    TOPN(1, gold_team_style, gold_team_style[xg_diff_per_game], DESC)
)
```

---

## Medidas — Match Predictor

```dax
Accuracy Modelo =
AVERAGE(gold_match_predictions[correct])

Partidas Previstas Corretamente =
CALCULATE(
    COUNT(gold_match_predictions[match_id]),
    gold_match_predictions[correct] = 1
)

Total Partidas =
COUNT(gold_match_predictions[match_id])

% Home Win Previsto =
AVERAGE(gold_match_predictions[p_home_win])
```
