"""
WorldCup Data Platform
Script: 03_openfootball_loader.py
===================================
Baixa o histórico COMPLETO de Copas do Mundo (1930-2022) do repositório
openfootball/world-cup no GitHub.

Dados reais por edição:
  - Grupos, fases, resultados de cada partida
  - Gols: minuto, jogador, tipo (gol normal, pênalti, gol contra)
  - Sedes e datas das partidas

Por que openfootball:
  - Sem API key, sem rate limit, dados estruturados em JSON/YAML
  - Cobre TODAS as 22 edições da Copa (1930-2022)
  - Atualizado pela comunidade, licença aberta

Saída:
  data/raw/historical/
  ├── wc_1930.json
  ├── wc_1934.json
  ├── ...
  └── wc_2022.json
  data/seeds/
  └── world_cups_index.json   ← metadados de todas as edições

Uso:
  python 03_openfootball_loader.py
  python 03_openfootball_loader.py --year 2022   # só uma edição
"""

import argparse
import json
import time
from pathlib import Path

import requests
from loguru import logger
from tqdm import tqdm

from config import (
    OPENFOOTBALL_BASE,
    OPENFOOTBALL_YEARS,
    DATA_RAW_DIR,
    DATA_SEEDS_DIR,
    REQUEST_HEADERS,
    REQUEST_DELAY_S,
)

OUTPUT_DIR = DATA_RAW_DIR / "historical"

# Mapeamento ano → arquivo no repositório openfootball
# Estrutura: https://github.com/openfootball/world-cup/tree/master
OPENFOOTBALL_FILES = {
    1930: "1930/worldcup.json",
    1934: "1934/worldcup.json",
    1938: "1938/worldcup.json",
    1950: "1950/worldcup.json",
    1954: "1954/worldcup.json",
    1958: "1958/worldcup.json",
    1962: "1962/worldcup.json",
    1966: "1966/worldcup.json",
    1970: "1970/worldcup.json",
    1974: "1974/worldcup.json",
    1978: "1978/worldcup.json",
    1982: "1982/worldcup.json",
    1986: "1986/worldcup.json",
    1990: "1990/worldcup.json",
    1994: "1994/worldcup.json",
    1998: "1998/worldcup.json",
    2002: "2002/worldcup.json",
    2006: "2006/worldcup.json",
    2010: "2010/worldcup.json",
    2014: "2014/worldcup.json",
    2018: "2018/worldcup.json",
    2022: "2022/worldcup.json",
}

# Metadados históricos curados (campeões, artilheiros confirmados)
WORLD_CUPS_INDEX = [
    {"year":1930,"host":"Uruguay",      "champion":"Uruguay",     "runner_up":"Argentina",      "third":"USA",          "matches":18, "goals":70,  "teams":13, "top_scorer":"Stábile",    "top_goals":8},
    {"year":1934,"host":"Italy",        "champion":"Italy",       "runner_up":"Czechoslovakia", "third":"Germany",      "matches":17, "goals":70,  "teams":16, "top_scorer":"Nejedlý",    "top_goals":5},
    {"year":1938,"host":"France",       "champion":"Italy",       "runner_up":"Hungary",        "third":"Brazil",       "matches":18, "goals":84,  "teams":15, "top_scorer":"Leônidas",   "top_goals":7},
    {"year":1950,"host":"Brazil",       "champion":"Uruguay",     "runner_up":"Brazil",         "third":"Sweden",       "matches":22, "goals":88,  "teams":13, "top_scorer":"Ademir",     "top_goals":9},
    {"year":1954,"host":"Switzerland",  "champion":"Germany",     "runner_up":"Hungary",        "third":"Austria",      "matches":26, "goals":140, "teams":16, "top_scorer":"Kocsis",     "top_goals":11},
    {"year":1958,"host":"Sweden",       "champion":"Brazil",      "runner_up":"Sweden",         "third":"France",       "matches":35, "goals":126, "teams":16, "top_scorer":"Fontaine",   "top_goals":13},
    {"year":1962,"host":"Chile",        "champion":"Brazil",      "runner_up":"Czechoslovakia", "third":"Chile",        "matches":32, "goals":89,  "teams":16, "top_scorer":"Garrincha",  "top_goals":4},
    {"year":1966,"host":"England",      "champion":"England",     "runner_up":"Germany",        "third":"Portugal",     "matches":32, "goals":89,  "teams":16, "top_scorer":"Eusébio",    "top_goals":9},
    {"year":1970,"host":"Mexico",       "champion":"Brazil",      "runner_up":"Italy",          "third":"Germany",      "matches":32, "goals":95,  "teams":16, "top_scorer":"Müller",     "top_goals":10},
    {"year":1974,"host":"Germany",      "champion":"Germany",     "runner_up":"Netherlands",    "third":"Poland",       "matches":38, "goals":97,  "teams":16, "top_scorer":"Lato",       "top_goals":7},
    {"year":1978,"host":"Argentina",    "champion":"Argentina",   "runner_up":"Netherlands",    "third":"Brazil",       "matches":38, "goals":102, "teams":16, "top_scorer":"Kempes",     "top_goals":6},
    {"year":1982,"host":"Spain",        "champion":"Italy",       "runner_up":"Germany",        "third":"Poland",       "matches":52, "goals":146, "teams":24, "top_scorer":"Rossi",      "top_goals":6},
    {"year":1986,"host":"Mexico",       "champion":"Argentina",   "runner_up":"Germany",        "third":"France",       "matches":52, "goals":132, "teams":24, "top_scorer":"Lineker",    "top_goals":6},
    {"year":1990,"host":"Italy",        "champion":"Germany",     "runner_up":"Argentina",      "third":"Italy",        "matches":52, "goals":115, "teams":24, "top_scorer":"Schillaci",  "top_goals":6},
    {"year":1994,"host":"USA",          "champion":"Brazil",      "runner_up":"Italy",          "third":"Sweden",       "matches":52, "goals":141, "teams":24, "top_scorer":"Stoichkov",  "top_goals":6},
    {"year":1998,"host":"France",       "champion":"France",      "runner_up":"Brazil",         "third":"Croatia",      "matches":64, "goals":171, "teams":32, "top_scorer":"Šuker",      "top_goals":6},
    {"year":2002,"host":"Japan/Korea",  "champion":"Brazil",      "runner_up":"Germany",        "third":"Turkey",       "matches":64, "goals":161, "teams":32, "top_scorer":"Ronaldo",    "top_goals":8},
    {"year":2006,"host":"Germany",      "champion":"Italy",       "runner_up":"France",         "third":"Germany",      "matches":64, "goals":147, "teams":32, "top_scorer":"Klose",      "top_goals":5},
    {"year":2010,"host":"South Africa", "champion":"Spain",       "runner_up":"Netherlands",    "third":"Germany",      "matches":64, "goals":145, "teams":32, "top_scorer":"T. Müller",  "top_goals":5},
    {"year":2014,"host":"Brazil",       "champion":"Germany",     "runner_up":"Argentina",      "third":"Netherlands",  "matches":64, "goals":171, "teams":32, "top_scorer":"Rodríguez",  "top_goals":6},
    {"year":2018,"host":"Russia",       "champion":"France",      "runner_up":"Croatia",        "third":"Belgium",      "matches":64, "goals":169, "teams":32, "top_scorer":"Kane",       "top_goals":6},
    {"year":2022,"host":"Qatar",        "champion":"Argentina",   "runner_up":"France",         "third":"Croatia",      "matches":64, "goals":172, "teams":32, "top_scorer":"Mbappé",     "top_goals":8},
]


def convert_to_old_format(new_data: dict) -> dict:
    """Converte o formato do repositório openfootball/worldcup.json para o formato esperado pelo pipeline (com rounds e team dicts)."""
    old_data = {
        "name": new_data.get("name", ""),
        "rounds": []
    }
    matches = new_data.get("matches", [])
    
    round_map = {}
    round_list = []
    
    for idx, m in enumerate(matches):
        round_name = m.get("group") or m.get("round") or "Unknown"
        if round_name not in round_map:
            round_map[round_name] = []
            round_list.append(round_name)
            
        goals = []
        for g in m.get("goals1", []):
            goals.append({
                "name": g.get("name", ""),
                "team": m.get("team1", ""),
                "minute": g.get("minute", 0)
            })
        for g in m.get("goals2", []):
            goals.append({
                "name": g.get("name", ""),
                "team": m.get("team2", ""),
                "minute": g.get("minute", 0)
            })
            
        old_match = {
            "num": idx + 1,
            "date": m.get("date", ""),
            "time": m.get("time", ""),
            "team1": {"name": m.get("team1", "")},
            "team2": {"name": m.get("team2", "")},
            "score": m.get("score", {}),
            "goals": goals
        }
        round_map[round_name].append(old_match)
        
    for r_name in round_list:
        old_data["rounds"].append({
            "name": r_name,
            "matches": round_map[r_name]
        })
        
    return old_data


def fetch_year(year: int, session: requests.Session) -> dict | None:
    """Baixa dados de uma edição da Copa do openfootball e converte para o formato esperado."""
    file_path = OPENFOOTBALL_FILES.get(year)
    if not file_path:
        logger.warning(f"Sem mapeamento para ano {year}")
        return None

    url = f"{OPENFOOTBALL_BASE}/{file_path}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        new_data = resp.json()
        data = convert_to_old_format(new_data)
        data["_meta"] = {
            "year": year,
            "source": "openfootball/world-cup",
            "url": url,
        }
        return data
    except requests.HTTPError as e:
        logger.error(f"HTTP {e.response.status_code} para {year}: {url}")
        return None
    except Exception as e:
        logger.error(f"Erro ao baixar {year}: {e}")
        return None


def main(args):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_SEEDS_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    years = [args.year] if args.year else OPENFOOTBALL_YEARS

    logger.info(f"Baixando histórico de {len(years)} Copas do Mundo...")

    success, failed = [], []
    for year in tqdm(years, desc="World Cups"):
        out_path = OUTPUT_DIR / f"wc_{year}.json"

        if out_path.exists():
            logger.info(f"  {year} já existe, pulando (use --force para rebaixar)")
            success.append(year)
            continue

        data = fetch_year(year, session)
        if data:
            with open(out_path, "w", encoding="utf-8") as f:
                import json
                json.dump(data, f, ensure_ascii=False, indent=2)
            n_matches = sum(
                len(r.get("matches", [])) for r in data.get("rounds", [])
            )
            logger.success(f"  {year}: {n_matches} partidas salvas")
            success.append(year)
        else:
            failed.append(year)

        time.sleep(REQUEST_DELAY_S)

    # ── Salva index de metadados
    index_path = DATA_SEEDS_DIR / "world_cups_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        import json
        json.dump(WORLD_CUPS_INDEX, f, ensure_ascii=False, indent=2)
    logger.success(f"Index salvo: {index_path} ({len(WORLD_CUPS_INDEX)} edições)")

    logger.success(
        f"\n{'='*50}\n"
        f"Histórico completo!\n"
        f"  ✓ Baixados: {len(success)} anos\n"
        f"  ✗ Falhas:   {len(failed)} anos {failed or ''}\n"
        f"  Path: {OUTPUT_DIR}\n"
        f"{'='*50}\n"
        f"Próximo: subir dados para DBFS (ver docs/PHASE_01_INGESTION.md)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download histórico real de Copas (1930-2022) — openfootball"
    )
    parser.add_argument("--year", type=int, default=None,
        help="Baixar apenas uma edição (ex: 2022)")
    parser.add_argument("--force", action="store_true",
        help="Rebaixar mesmo se o arquivo já existe")
    args = parser.parse_args()
    main(args)
