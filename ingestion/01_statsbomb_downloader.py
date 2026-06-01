"""
WorldCup Data Platform
Script: 01_statsbomb_downloader.py
====================================
Baixa dados REAIS do StatsBomb Open Data (GitHub) para todas as
competições configuradas em config.py.

O que é baixado por partida:
  - events/     → ~3.400 eventos por jogo (passes, chutes, faltas, gols, pressão...)
  - lineups/    → escalações confirmadas com posições
  - three-sixty/ → dados 360° (freeze frame com posição de todos jogadores visíveis)

Estrutura de saída:
  data/raw/statsbomb/
  ├── competitions.json
  ├── wc_2022/
  │   ├── matches.json
  │   ├── events/
  │   │   ├── {match_id}.json
  │   │   └── ...
  │   ├── lineups/
  │   │   └── {match_id}.json
  │   └── three-sixty/
  │       └── {match_id}.json   (só se has_360=True)
  ├── euro_2024/
  │   └── ...
  └── ...

Uso:
  pip install statsbombpy tqdm loguru
  python 01_statsbomb_downloader.py
  python 01_statsbomb_downloader.py --competition-id 43 --season-id 106  # só WC 2022
  python 01_statsbomb_downloader.py --dry-run   # mostra o que seria baixado

Por que StatsBombPy:
  - Wrapper oficial do StatsBomb para Python
  - Converte JSONs aninhados em DataFrames prontos
  - Cuida do rate limiting contra o GitHub automaticamente
"""

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm
from statsbombpy import sb

from config import STATSBOMB_COMPETITIONS, DATA_RAW_DIR

# ── Slugs para nomes de pasta
COMP_SLUGS = {
    (43, 106): "wc_2022",
    (43, 3):   "wc_2018",
    (55, 282): "euro_2024",
    (55, 43):  "euro_2020",
    (223, 282): "copa_america_2024",
}


def save_json(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, pd.DataFrame):
            f.write(data.to_json(orient="records", force_ascii=False, indent=2))
        elif isinstance(data, (list, dict)):
            json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def download_competition(comp: dict, output_base: Path, dry_run: bool = False):
    cid = comp["competition_id"]
    sid = comp["season_id"]
    name = comp["name"]
    has_360 = comp["has_360"]
    slug = COMP_SLUGS.get((cid, sid), f"comp_{cid}_season_{sid}")
    out_dir = output_base / slug

    logger.info(f"[{name}] competition_id={cid} season_id={sid} 360={has_360}")

    # ── 1. Matches
    logger.info(f"[{name}] Fetching match list...")
    matches_df = sb.matches(competition_id=cid, season_id=sid)
    n_matches = len(matches_df)
    logger.info(f"[{name}] {n_matches} matches found")

    if dry_run:
        logger.info(f"[DRY RUN] Would download {n_matches} matches from {name}")
        return

    matches_path = save_json(matches_df, out_dir / "matches.json")
    logger.success(f"Saved: {matches_path}")

    match_ids = matches_df["match_id"].tolist()

    # ── 2. Events (todos os eventos de cada partida)
    events_dir = out_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{name}] Downloading events for {n_matches} matches...")
    for match_id in tqdm(match_ids, desc=f"{slug}/events"):
        out_path = events_dir / f"{match_id}.json"
        if out_path.exists():
            continue  # skip se já baixou (idempotente)
        try:
            events_df = sb.events(match_id=match_id)
            save_json(events_df, out_path)
            time.sleep(0.3)  # gentil com o GitHub
        except Exception as e:
            logger.error(f"[{name}] match_id={match_id} events failed: {e}")

    # ── 3. Lineups (escalações confirmadas)
    lineups_dir = out_dir / "lineups"
    lineups_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{name}] Downloading lineups...")
    for match_id in tqdm(match_ids, desc=f"{slug}/lineups"):
        out_path = lineups_dir / f"{match_id}.json"
        if out_path.exists():
            continue
        try:
            lineups = sb.lineups(match_id=match_id)
            # lineups é dict: {team_name: DataFrame}
            serializable = {
                team: df.to_dict(orient="records")
                for team, df in lineups.items()
            }
            save_json(serializable, out_path)
            time.sleep(0.3)
        except Exception as e:
            logger.error(f"[{name}] match_id={match_id} lineups failed: {e}")

    # ── 4. StatsBomb 360 (posição de todos jogadores por evento)
    if has_360:
        sb360_dir = out_dir / "three-sixty"
        sb360_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"[{name}] Downloading 360 freeze frames...")
        for match_id in tqdm(match_ids, desc=f"{slug}/360"):
            out_path = sb360_dir / f"{match_id}.json"
            if out_path.exists():
                continue
            try:
                frames_df = sb.frames(match_id=match_id)
                save_json(frames_df, out_path)
                time.sleep(0.5)  # 360 é mais pesado
            except Exception as e:
                logger.warning(f"[{name}] match_id={match_id} 360 not available: {e}")

    # ── Resumo
    n_events  = len(list(events_dir.glob("*.json")))
    n_lineups = len(list(lineups_dir.glob("*.json")))
    logger.success(
        f"[{name}] ✓ Done — "
        f"{n_matches} matches | {n_events} event files | {n_lineups} lineup files"
    )


def main(args):
    output_base = DATA_RAW_DIR / "statsbomb"
    output_base.mkdir(parents=True, exist_ok=True)

    # ── competitions.json (mapa completo disponível no StatsBomb)
    comps_path = output_base / "competitions.json"
    if not comps_path.exists():
        logger.info("Fetching competitions list...")
        comps_df = sb.competitions()
        save_json(comps_df, comps_path)
        logger.success(f"Saved competitions.json ({len(comps_df)} competitions)")

    # ── Filtrar por args se fornecidos
    targets = STATSBOMB_COMPETITIONS
    if args.competition_id and args.season_id:
        targets = [
            c for c in targets
            if c["competition_id"] == args.competition_id
            and c["season_id"] == args.season_id
        ]
        if not targets:
            logger.error(
                f"competition_id={args.competition_id} season_id={args.season_id} "
                f"não encontrado em STATSBOMB_COMPETITIONS no config.py"
            )
            return

    logger.info(f"Downloading {len(targets)} competition(s)...")
    for comp in targets:
        download_competition(comp, output_base, dry_run=args.dry_run)

    # ── Relatório final
    total_files = sum(1 for _ in output_base.rglob("*.json"))
    total_mb = sum(
        f.stat().st_size for f in output_base.rglob("*.json")
    ) / 1024 / 1024

    logger.success(
        f"\n{'='*50}\n"
        f"Download completo!\n"
        f"  Arquivos: {total_files}\n"
        f"  Tamanho: {total_mb:.1f} MB\n"
        f"  Path: {output_base}\n"
        f"{'='*50}\n"
        f"Próximo: python 02_fbref_scraper.py"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download StatsBomb Open Data (real football event data)"
    )
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--season-id",      type=int, default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra o que seria baixado sem baixar")
    args = parser.parse_args()
    main(args)
