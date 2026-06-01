"""
WorldCup Data Platform — Configuração Central
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR       = Path(__file__).parent.parent
DATA_RAW_DIR   = ROOT_DIR / "data" / "raw"
DATA_SEEDS_DIR = ROOT_DIR / "data" / "seeds"
DBFS_BASE      = "dbfs:/FileStore/wc-platform"

# ── StatsBomb competitions alvo
STATSBOMB_COMPETITIONS = [
    {"competition_id": 43,  "season_id": 106, "name": "FIFA World Cup 2022",  "has_360": True},
    {"competition_id": 43,  "season_id": 3,   "name": "FIFA World Cup 2018",  "has_360": False},
    {"competition_id": 55,  "season_id": 282, "name": "UEFA Euro 2024",       "has_360": True},
    {"competition_id": 55,  "season_id": 43,  "name": "UEFA Euro 2020",       "has_360": True},
    {"competition_id": 223, "season_id": 282, "name": "Copa América 2024",    "has_360": False},
]

# ── FBref — top-5 ligas temporada 2024-25
FBREF_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "GER-Bundesliga",
    "ITA-Serie A",
    "FRA-Ligue 1",
]
FBREF_SEASON = "2024-25"

# ── football-data.org
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
FOOTBALL_DATA_BASE    = "https://api.football-data.org/v4"

# ── openfootball worldcup histórico
OPENFOOTBALL_BASE  = "https://raw.githubusercontent.com/openfootball/worldcup.json/master"
OPENFOOTBALL_YEARS = list(range(1930, 2023, 4))

REQUEST_HEADERS = {
    "User-Agent": "WorldCup-DataPlatform/2.0 (portfolio; educational use)"
}
REQUEST_DELAY_S = 3.0
