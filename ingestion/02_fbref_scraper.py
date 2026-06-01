"""
WorldCup Data Platform
Script: 02_fbref_scraper.py
=============================
Coleta estatísticas REAIS de jogadores das top-5 ligas europeias
(temporada 2024-25) via FBref usando a biblioteca `soccerdata`.

O que é coletado por liga:
  - Shooting: gols, chutes, xG, xG/90, chutes no alvo
  - Passing: passes, key passes, xA, progressive passes
  - Defense: bloqueios, interceptações, pressões, PPDA
  - Possession: dribles, condução, progressive carries
  - Misc: cartões, faltas, duelos aéreos ganhos

Por que isso importa para o projeto:
  Jogadores que vão à Copa 2026 têm suas stats de clube como
  feature para os modelos de ML (xG no clube → predictor de gols na Copa).

Saída:
  data/raw/fbref/
  ├── shooting_2024-25.parquet
  ├── passing_2024-25.parquet
  ├── defense_2024-25.parquet
  ├── possession_2024-25.parquet
  └── misc_2024-25.parquet

Uso:
  pip install soccerdata loguru tqdm
  python 02_fbref_scraper.py
  python 02_fbref_scraper.py --stat shooting   # só uma categoria
  python 02_fbref_scraper.py --league "ENG-Premier League"  # só uma liga
"""

import argparse
import time
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm

try:
    import soccerdata as sd
except ImportError:
    raise ImportError(
        "soccerdata não instalado. Execute: pip install soccerdata"
    )

from config import FBREF_LEAGUES, FBREF_SEASON, DATA_RAW_DIR

OUTPUT_DIR = DATA_RAW_DIR / "fbref"

# ── Categorias de estatística disponíveis no FBref
STAT_CATEGORIES = {
    "shooting":    "Shooting stats — gols, xG, chutes, xG/90",
    "passing":     "Passing stats — passes, key passes, xA, prog passes",
    "defense":     "Defense stats — tackles, bloqueios, interceptações, pressões",
    "possession":  "Possession — dribles, progressive carries, touches",
    "misc":        "Misc — cartões, faltas, duelos aéreos",
}


def fetch_stat(fbref: sd.FBref, stat: str, league: str) -> pd.DataFrame | None:
    """
    Busca uma categoria de stat para uma liga.
    Trata erros graciosamente — FBref às vezes retorna 429 ou dados vazios.
    """
    try:
        df = fbref.read_player_season_stats(stat_type=stat)
        df["stat_category"] = stat
        df["source_league"]  = league
        df["source_season"]  = FBREF_SEASON
        logger.info(f"  ✓ {stat}: {len(df)} player records")
        return df
    except Exception as e:
        logger.warning(f"  ✗ {stat} @ {league}: {e}")
        return None


def scrape_league(league: str, stats: list[str]) -> dict[str, pd.DataFrame]:
    """Scrape todas as categorias de stat para uma liga."""
    logger.info(f"\n[FBref] Liga: {league}")

    try:
        fbref = sd.FBref(leagues=league, seasons=FBREF_SEASON)
    except Exception as e:
        logger.error(f"Falha ao inicializar FBref para {league}: {e}")
        return {}

    results = {}
    for stat in tqdm(stats, desc=f"  {league}"):
        df = fetch_stat(fbref, stat, league)
        if df is not None and not df.empty:
            results[stat] = df
        time.sleep(2.0)  # respeitar rate limit do FBref

    return results


def main(args):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Filtros por args
    leagues = [args.league] if args.league else FBREF_LEAGUES
    stats   = [args.stat]   if args.stat   else list(STAT_CATEGORIES.keys())

    logger.info(f"FBref scraper iniciado")
    logger.info(f"  Ligas:    {leagues}")
    logger.info(f"  Stats:    {stats}")
    logger.info(f"  Temporada: {FBREF_SEASON}")

    # ── Coleta por liga
    all_dfs: dict[str, list[pd.DataFrame]] = {s: [] for s in stats}

    for league in leagues:
        league_data = scrape_league(league, stats)
        for stat, df in league_data.items():
            all_dfs[stat].append(df)
        time.sleep(3.0)  # pausa entre ligas

    # ── Consolida e salva por categoria
    saved = []
    for stat, dfs in all_dfs.items():
        if not dfs:
            logger.warning(f"Sem dados para stat={stat}")
            continue

        combined = pd.concat(dfs, ignore_index=True)

        # Normalizar colunas multi-level do pandas (FBref usa MultiIndex)
        if isinstance(combined.columns, pd.MultiIndex):
            combined.columns = [
                "_".join(filter(None, map(str, col))).strip()
                for col in combined.columns
            ]

        out_path = OUTPUT_DIR / f"{stat}_{FBREF_SEASON}.parquet"
        combined.to_parquet(out_path, index=False)
        saved.append(out_path)
        logger.success(
            f"Salvo: {out_path.name} "
            f"({len(combined)} registros, {combined['source_league'].nunique()} ligas)"
        )

    # ── Relatório
    total_mb = sum(p.stat().st_size for p in saved) / 1024 / 1024
    logger.success(
        f"\n{'='*50}\n"
        f"FBref scraping completo!\n"
        f"  Arquivos: {len(saved)}\n"
        f"  Tamanho total: {total_mb:.1f} MB\n"
        f"  Path: {OUTPUT_DIR}\n"
        f"{'='*50}\n"
        f"Próximo: python 03_openfootball_loader.py"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Coleta stats reais de jogadores do FBref (soccerdata)"
    )
    parser.add_argument("--league", default=None,
        help="Filtrar por liga (ex: 'ENG-Premier League')")
    parser.add_argument("--stat",   default=None,
        choices=list(STAT_CATEGORIES.keys()),
        help="Filtrar por categoria de stat")
    args = parser.parse_args()
    main(args)
