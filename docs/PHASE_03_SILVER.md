# Fase 03 — Silver

**Status:** ✅ Completa  
**Tabelas geradas:** 6  
**Executar após:** Fase 02 — Bronze

---

## Ordem de execução

| Ordem | Notebook | Tabela Silver | Depende de |
|---|---|---|---|
| 1 | `01_silver_match_timeline.py` | `worldcup.silver.match_timeline` | bronze.statsbomb_events + bronze.statsbomb_matches |
| 2 | `02_silver_player_stats.py` | `worldcup.silver.player_match_stats` | silver.match_timeline + bronze.statsbomb_lineups + bronze.fbref_player_season |
| 3 | `03_silver_team_performance.py` | `worldcup.silver.team_performance` | silver.match_timeline + bronze.statsbomb_matches |
| 4 | `04_silver_shot_map.py` | `worldcup.silver.shot_map` | silver.match_timeline + bronze.statsbomb_360 |
| 5 | `05_silver_elo_ratings.py` | `worldcup.silver.elo_ratings` + `worldcup.silver.elo_current_snapshot` | bronze.historical_results |

---

## O que cada tabela entrega

### match_timeline
- 1 linha por evento (filtrado — sem ruído operacional do StatsBomb)
- Distância e ângulo ao gol calculados para cada evento com localização
- Placar acumulado no momento de cada evento (window function)
- Zona do campo (`defensive_third`, `middle_third`, `attacking_third`)
- MERGE por `event_id + match_id`

### player_match_stats
- 1 linha por jogador por partida
- Agrega: shots, goals, xG, passes, pass%, xA, key_passes, assists, pressures, carries progressivos, duels
- Enriquecida com stats de clube do FBref (xG/90, npxG temporada 2024-25)
- MERGE por `player_id + match_id`

### team_performance
- 1 linha por time por partida
- PPDA (intensidade de pressing), posse %, xG, xGA, xG diferencial
- Progressive passes, carries, crosses, profundidade defensiva
- Resultado (W/D/L) calculado
- MERGE por `team_id + match_id`

### shot_map
- Todos os chutes com xG + contexto 360° onde disponível
- Features táticas: `defenders_in_cone`, `distance_nearest_defender`, `gk_distance_to_goalline`
- Flag `has_360_data` — True para WC 2022, Euro 2024, Euro 2020
- Base para o modelo xG da Fase ML
- MERGE por `event_id + match_id`

### elo_ratings
- ELO calculado iterativamente para cada partida de 1930 a 2022
- K-factor diferenciado por fase (50 grupos, 55 oitavas, 60 final/semi)
- `elo_current_snapshot`: estado atual do ELO de cada seleção (feature #1 do ML)

---

## Princípios desta camada

1. **MERGE upsert em todas as tabelas** — idempotente, pode reprocessar sem duplicar
2. **Enriquecimento cruzado** — player_stats usa lineups + FBref; shot_map usa 360°
3. **Features derivadas** — distância ao gol, ângulo, zona, PPDA, ELO calculados aqui
4. **Fallback Unity Catalog** — todos os notebooks tentam UC, caem em hive_metastore
5. **Sem dados descartados** — Bronze preserva tudo; Silver seleciona e transforma

---

## Próxima Fase

**Fase 04 — Gold**: Agregações finais, feature store para ML, standings, top artilheiros.

Arquivo: `docs/PHASE_04_GOLD.md`
