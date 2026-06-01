# Fase 01 — Ingestion (Dados Reais)

**Status:** ✅ Completa  
**Dados:** 100% reais — sem simulação  
**Fontes:** StatsBomb Open Data · FBref (soccerdata) · openfootball

---

## O que é coletado

| Script | Fonte | Dados | Volume estimado |
|---|---|---|---|
| `01_statsbomb_downloader.py` | StatsBomb GitHub | Eventos, lineups, 360° — 5 competições | ~1.5 GB JSON |
| `02_fbref_scraper.py` | FBref via soccerdata | Stats de jogador top-5 ligas 2024-25 | ~50 MB Parquet |
| `03_openfootball_loader.py` | openfootball/world-cup | Resultados 1930-2022 | ~10 MB JSON |

---

## StatsBomb — Competições e IDs exatos

| Competição | competition_id | season_id | Jogos | 360°? |
|---|---|---|---|---|
| FIFA World Cup 2022 | 43 | 106 | 64 | ✅ |
| FIFA World Cup 2018 | 43 | 3 | 64 | ❌ |
| UEFA Euro 2024 | 55 | 282 | 51 | ✅ |
| UEFA Euro 2020 | 55 | 43 | 51 | ✅ |
| Copa América 2024 | 223 | 282 | 32 | ❌ |

**Total: 262 partidas reais com ~3.400 eventos cada**

---

## O que cada evento StatsBomb contém

### events/{match_id}.json — por evento:
- `type`: Pass, Shot, Carry, Pressure, Duel, Foul, Goal Keeper...
- `location`: [x, y] coordenadas no campo (0-120 × 0-80)
- `timestamp`: minuto:segundo exato
- Para **chutes**: `shot.xg`, `shot.outcome`, `shot.technique`, `shot.body_part`
- Para **passes**: `pass.length`, `pass.angle`, `pass.recipient`, `pass.xa`
- Para **pressões**: `pressure.counterpress` (pressing após perda)

### three-sixty/{match_id}.json — dados táticos:
- Posição [x,y] de TODOS jogadores visíveis no frame de cada evento
- Permite calcular: distância do defensor mais próximo no momento do chute,
  espaço entre linhas, intensidade de pressing contextual

---

## Execução — Passo a Passo

```bash
# 0. Instalar dependências
pip install -r requirements.txt

cd ingestion/

# 1. StatsBomb — todas as 5 competições
python 01_statsbomb_downloader.py
# Só WC 2022:
python 01_statsbomb_downloader.py --competition-id 43 --season-id 106
# Dry run (sem baixar):
python 01_statsbomb_downloader.py --dry-run

# 2. FBref — stats de jogadores nas top-5 ligas 2024-25
python 02_fbref_scraper.py
# Só Premier League:
python 02_fbref_scraper.py --league "ENG-Premier League"
# Só shooting stats:
python 02_fbref_scraper.py --stat shooting

# 3. Histórico de Copas 1930-2022
python 03_openfootball_loader.py
# Só 2022:
python 03_openfootball_loader.py --year 2022
```

---

## Upload para DBFS (Databricks Community Edition)

```python
# Opção A: Upload via UI do Databricks
# Data → Add Data → DBFS → /FileStore/wc-platform/

# Opção B: Databricks CLI
# databricks fs cp -r data/raw/ dbfs:/FileStore/wc-platform/raw/ --overwrite

# Verificar no notebook:
dbutils.fs.ls("dbfs:/FileStore/wc-platform/raw/statsbomb/")
```

---

## Próxima Fase

**Fase 02 — Bronze**: Notebooks que leem do DBFS e escrevem Delta tables em `worldcup.bronze.*`

