"""
WorldCup Data Platform
Producer: static_loader.py
===========================
Gera e salva os dados estáticos (seeds) em `data/seeds/`.
Esses arquivos são enviados ao DBFS via `scripts/upload_to_dbfs.sh`
para ingestão no notebook Bronze.

Execução única — não usa Kafka.

Uso:
  python static_loader.py --output-dir ../data/seeds
"""

import argparse
import json
import os
from datetime import datetime
from loguru import logger
from config import TEAMS

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "seeds")


# ── FIFA Rankings (top 32 + seleções da Copa)
FIFA_RANKINGS = [
    {"rank": 1,  "team": "Argentina",    "id": "ARG", "points": 1851.42, "confederation": "CONMEBOL"},
    {"rank": 2,  "team": "France",       "id": "FRA", "points": 1840.76, "confederation": "UEFA"},
    {"rank": 3,  "team": "Belgium",      "id": "BEL", "points": 1781.14, "confederation": "UEFA"},
    {"rank": 4,  "team": "England",      "id": "ENG", "points": 1764.22, "confederation": "UEFA"},
    {"rank": 5,  "team": "Brazil",       "id": "BRA", "points": 1756.08, "confederation": "CONMEBOL"},
    {"rank": 6,  "team": "Portugal",     "id": "POR", "points": 1741.35, "confederation": "UEFA"},
    {"rank": 7,  "team": "Netherlands",  "id": "NED", "points": 1731.20, "confederation": "UEFA"},
    {"rank": 8,  "team": "Spain",        "id": "ESP", "points": 1718.55, "confederation": "UEFA"},
    {"rank": 9,  "team": "Croatia",      "id": "CRO", "points": 1706.44, "confederation": "UEFA"},
    {"rank": 10, "team": "Italy",        "id": "ITA", "points": 1698.90, "confederation": "UEFA"},
    {"rank": 11, "team": "Mexico",       "id": "MEX", "points": 1672.33, "confederation": "CONCACAF"},
    {"rank": 12, "team": "Morocco",      "id": "MAR", "points": 1664.12, "confederation": "CAF"},
    {"rank": 13, "team": "USA",          "id": "USA", "points": 1650.44, "confederation": "CONCACAF"},
    {"rank": 14, "team": "Uruguay",      "id": "URU", "points": 1645.71, "confederation": "CONMEBOL"},
    {"rank": 16, "team": "Germany",      "id": "GER", "points": 1632.88, "confederation": "UEFA"},
    {"rank": 17, "team": "Japan",        "id": "JPN", "points": 1615.20, "confederation": "AFC"},
    {"rank": 18, "team": "Senegal",      "id": "SEN", "points": 1608.74, "confederation": "CAF"},
    {"rank": 20, "team": "Switzerland",  "id": "SUI", "points": 1595.30, "confederation": "UEFA"},
    {"rank": 22, "team": "Denmark",      "id": "DEN", "points": 1572.16, "confederation": "UEFA"},
    {"rank": 23, "team": "Australia",    "id": "AUS", "points": 1556.43, "confederation": "AFC"},
    {"rank": 25, "team": "Iran",         "id": "IRN", "points": 1540.88, "confederation": "AFC"},
    {"rank": 26, "team": "South Korea",  "id": "KOR", "points": 1525.67, "confederation": "AFC"},
    {"rank": 28, "team": "Poland",       "id": "POL", "points": 1510.42, "confederation": "UEFA"},
    {"rank": 33, "team": "Serbia",       "id": "SRB", "points": 1485.90, "confederation": "UEFA"},
    {"rank": 38, "team": "Qatar",        "id": "QAT", "points": 1450.22, "confederation": "AFC"},
    {"rank": 40, "team": "Peru",         "id": "PER", "points": 1440.15, "confederation": "CONMEBOL"},
    {"rank": 41, "team": "Costa Rica",   "id": "CRC", "points": 1435.78, "confederation": "CONCACAF"},
    {"rank": 44, "team": "Ecuador",      "id": "ECU", "points": 1420.33, "confederation": "CONMEBOL"},
    {"rank": 52, "team": "Ivory Coast",  "id": "CIV", "points": 1388.44, "confederation": "CAF"},
    {"rank": 55, "team": "Cameroon",     "id": "CMR", "points": 1370.22, "confederation": "CAF"},
    {"rank": 60, "team": "Ghana",        "id": "GHA", "points": 1355.88, "confederation": "CAF"},
    {"rank": 63, "team": "Canada",       "id": "CAN", "points": 1340.11, "confederation": "CONCACAF"},
]


# ── Histórico de Copas do Mundo
WORLD_CUPS_HISTORY = [
    {"year": 1930, "host": "Uruguay",       "champion": "Uruguay",     "runner_up": "Argentina",   "third": "USA",          "top_scorer": "Guillermo Stábile", "top_scorer_goals": 8,  "teams": 13, "matches": 18,  "total_goals": 70},
    {"year": 1934, "host": "Italy",         "champion": "Italy",       "runner_up": "Czechoslovakia","third": "Germany",     "top_scorer": "Oldřich Nejedlý",   "top_scorer_goals": 5,  "teams": 16, "matches": 17,  "total_goals": 70},
    {"year": 1938, "host": "France",        "champion": "Italy",       "runner_up": "Hungary",     "third": "Brazil",       "top_scorer": "Leônidas",          "top_scorer_goals": 7,  "teams": 15, "matches": 18,  "total_goals": 84},
    {"year": 1950, "host": "Brazil",        "champion": "Uruguay",     "runner_up": "Brazil",      "third": "Sweden",       "top_scorer": "Ademir",            "top_scorer_goals": 9,  "teams": 13, "matches": 22,  "total_goals": 88},
    {"year": 1954, "host": "Switzerland",   "champion": "Germany",     "runner_up": "Hungary",     "third": "Austria",      "top_scorer": "Sándor Kocsis",     "top_scorer_goals": 11, "teams": 16, "matches": 26,  "total_goals": 140},
    {"year": 1958, "host": "Sweden",        "champion": "Brazil",      "runner_up": "Sweden",      "third": "France",       "top_scorer": "Just Fontaine",     "top_scorer_goals": 13, "teams": 16, "matches": 35,  "total_goals": 126},
    {"year": 1962, "host": "Chile",         "champion": "Brazil",      "runner_up": "Czechoslovakia","third": "Chile",       "top_scorer": "Garrincha",         "top_scorer_goals": 4,  "teams": 16, "matches": 32,  "total_goals": 89},
    {"year": 1966, "host": "England",       "champion": "England",     "runner_up": "Germany",     "third": "Portugal",     "top_scorer": "Eusébio",           "top_scorer_goals": 9,  "teams": 16, "matches": 32,  "total_goals": 89},
    {"year": 1970, "host": "Mexico",        "champion": "Brazil",      "runner_up": "Italy",       "third": "Germany",      "top_scorer": "Gerd Müller",       "top_scorer_goals": 10, "teams": 16, "matches": 32,  "total_goals": 95},
    {"year": 1974, "host": "Germany",       "champion": "Germany",     "runner_up": "Netherlands", "third": "Poland",       "top_scorer": "Grzegorz Lato",     "top_scorer_goals": 7,  "teams": 16, "matches": 38,  "total_goals": 97},
    {"year": 1978, "host": "Argentina",     "champion": "Argentina",   "runner_up": "Netherlands", "third": "Brazil",       "top_scorer": "Mario Kempes",      "top_scorer_goals": 6,  "teams": 16, "matches": 38,  "total_goals": 102},
    {"year": 1982, "host": "Spain",         "champion": "Italy",       "runner_up": "Germany",     "third": "Poland",       "top_scorer": "Paolo Rossi",       "top_scorer_goals": 6,  "teams": 24, "matches": 52,  "total_goals": 146},
    {"year": 1986, "host": "Mexico",        "champion": "Argentina",   "runner_up": "Germany",     "third": "France",       "top_scorer": "Gary Lineker",      "top_scorer_goals": 6,  "teams": 24, "matches": 52,  "total_goals": 132},
    {"year": 1990, "host": "Italy",         "champion": "Germany",     "runner_up": "Argentina",   "third": "Italy",        "top_scorer": "Salvatore Schillaci","top_scorer_goals": 6, "teams": 24, "matches": 52,  "total_goals": 115},
    {"year": 1994, "host": "USA",           "champion": "Brazil",      "runner_up": "Italy",       "third": "Sweden",       "top_scorer": "Hristo Stoichkov",  "top_scorer_goals": 6,  "teams": 24, "matches": 52,  "total_goals": 141},
    {"year": 1998, "host": "France",        "champion": "France",      "runner_up": "Brazil",      "third": "Croatia",      "top_scorer": "Davor Šuker",       "top_scorer_goals": 6,  "teams": 32, "matches": 64,  "total_goals": 171},
    {"year": 2002, "host": "Japan/S.Korea", "champion": "Brazil",      "runner_up": "Germany",     "third": "Turkey",       "top_scorer": "Ronaldo",           "top_scorer_goals": 8,  "teams": 32, "matches": 64,  "total_goals": 161},
    {"year": 2006, "host": "Germany",       "champion": "Italy",       "runner_up": "France",      "third": "Germany",      "top_scorer": "Miroslav Klose",    "top_scorer_goals": 5,  "teams": 32, "matches": 64,  "total_goals": 147},
    {"year": 2010, "host": "South Africa",  "champion": "Spain",       "runner_up": "Netherlands", "third": "Germany",      "top_scorer": "Thomas Müller",     "top_scorer_goals": 5,  "teams": 32, "matches": 64,  "total_goals": 145},
    {"year": 2014, "host": "Brazil",        "champion": "Germany",     "runner_up": "Argentina",   "third": "Netherlands",  "top_scorer": "James Rodríguez",   "top_scorer_goals": 6,  "teams": 32, "matches": 64,  "total_goals": 171},
    {"year": 2018, "host": "Russia",        "champion": "France",      "runner_up": "Croatia",     "third": "Belgium",      "top_scorer": "Harry Kane",        "top_scorer_goals": 6,  "teams": 32, "matches": 64,  "total_goals": 169},
    {"year": 2022, "host": "Qatar",         "champion": "Argentina",   "runner_up": "France",      "third": "Croatia",      "top_scorer": "Kylian Mbappé",     "top_scorer_goals": 8,  "teams": 32, "matches": 64,  "total_goals": 172},
    {"year": 2026, "host": "USA/MEX/CAN",   "champion": None,          "runner_up": None,          "third": None,           "top_scorer": None,                "top_scorer_goals": None,"teams": 48, "matches": 104, "total_goals": None},
]


# ── Elencos Copa 2026 (selecionados principais)
def generate_squads() -> list[dict]:
    """Gera elencos sintéticos para as principais seleções."""
    squads = []
    key_players = {
        "BRA": [
            {"name": "Alisson Becker",  "pos": "GK", "age": 33, "club": "Liverpool",         "market_value_m": 25},
            {"name": "Marquinhos",      "pos": "CB", "age": 30, "club": "PSG",               "market_value_m": 40},
            {"name": "Thiago Silva",    "pos": "CB", "age": 41, "club": "Fluminense",         "market_value_m": 2},
            {"name": "Danilo",          "pos": "RB", "age": 33, "club": "Juventus",           "market_value_m": 8},
            {"name": "Casemiro",        "pos": "DM", "age": 32, "club": "Manchester United",  "market_value_m": 20},
            {"name": "Lucas Paquetá",   "pos": "AM", "age": 27, "club": "West Ham",           "market_value_m": 60},
            {"name": "Rodrygo",         "pos": "FW", "age": 23, "club": "Real Madrid",        "market_value_m": 100},
            {"name": "Vinícius Jr.",    "pos": "FW", "age": 24, "club": "Real Madrid",        "market_value_m": 180},
            {"name": "Richarlison",     "pos": "FW", "age": 27, "club": "Tottenham",          "market_value_m": 50},
            {"name": "Raphinha",        "pos": "FW", "age": 27, "club": "Barcelona",          "market_value_m": 70},
            {"name": "Endrick",         "pos": "FW", "age": 18, "club": "Real Madrid",        "market_value_m": 60},
        ],
        "ARG": [
            {"name": "Emiliano Martínez","pos": "GK", "age": 32, "club": "Aston Villa",      "market_value_m": 30},
            {"name": "Lionel Messi",    "pos": "FW", "age": 39, "club": "Inter Miami",        "market_value_m": 20},
            {"name": "Julián Álvarez",  "pos": "FW", "age": 25, "club": "Atlético Madrid",   "market_value_m": 90},
            {"name": "Rodrigo De Paul", "pos": "CM", "age": 30, "club": "Atlético Madrid",   "market_value_m": 40},
            {"name": "Lautaro Martínez","pos": "FW", "age": 27, "club": "Inter Milan",       "market_value_m": 100},
        ],
        "FRA": [
            {"name": "Mike Maignan",    "pos": "GK", "age": 29, "club": "AC Milan",          "market_value_m": 55},
            {"name": "Kylian Mbappé",   "pos": "FW", "age": 27, "club": "Real Madrid",       "market_value_m": 200},
            {"name": "Antoine Griezmann","pos":"FW", "age": 35, "club": "Atlético Madrid",   "market_value_m": 20},
            {"name": "Aurélien Tchouaméni","pos":"DM","age": 25, "club": "Real Madrid",      "market_value_m": 100},
        ],
        "ENG": [
            {"name": "Jude Bellingham", "pos": "AM", "age": 21, "club": "Real Madrid",       "market_value_m": 180},
            {"name": "Harry Kane",      "pos": "FW", "age": 32, "club": "Bayern Munich",     "market_value_m": 80},
            {"name": "Phil Foden",      "pos": "AM", "age": 26, "club": "Manchester City",   "market_value_m": 150},
            {"name": "Bukayo Saka",     "pos": "FW", "age": 22, "club": "Arsenal",           "market_value_m": 160},
        ],
    }

    for team_id, players in key_players.items():
        for i, p in enumerate(players):
            squads.append({
                "player_id":       f"{team_id.lower()}_{p['name'].lower().replace(' ', '_')}",
                "player_name":     p["name"],
                "team_id":         team_id,
                "position":        p["pos"],
                "age":             p["age"],
                "club":            p["club"],
                "market_value_m":  p["market_value_m"],
                "squad_number":    i + 1,
                "wc_2026":         True,
            })
    return squads


def save_json(data: list | dict, filename: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.success(f"Saved: {path} ({len(data) if isinstance(data, list) else 1} records)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WorldCup static data loader")
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    logger.info(f"Generating seeds → {args.output_dir}")

    save_json(FIFA_RANKINGS,        "fifa_rankings.json",      args.output_dir)
    save_json(WORLD_CUPS_HISTORY,   "world_cups_history.json", args.output_dir)
    save_json(generate_squads(),    "wc2026_squads.json",      args.output_dir)
    save_json(TEAMS,                "wc2026_teams.json",       args.output_dir)

    logger.info("All seeds generated. Next: bash scripts/upload_to_dbfs.sh")
