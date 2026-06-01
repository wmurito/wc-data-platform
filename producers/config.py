"""
WorldCup Data Platform — Configuração Central dos Producers
============================================================
Lê variáveis de ambiente ou usa defaults para desenvolvimento local.
Copie `.env.example` para `.env` e preencha os valores reais.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "match_events": "wc.match.events",
    "odds":         "wc.odds.live",
    "weather":      "wc.weather",
    "lineups":      "wc.lineups",
}

KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "retry.backoff.ms": 500,
    "compression.type": "lz4",
    "linger.ms": 50,
    "batch.size": 16384,
}

# ── API-Football (RapidAPI) — opcional para portfólio
# Se não tiver key, producers usam dados simulados automaticamente
API_FOOTBALL_KEY  = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_HOST = "api-football-v1.p.rapidapi.com"
API_FOOTBALL_BASE = f"https://{API_FOOTBALL_HOST}"

# Copa do Mundo 2026: league_id = 1 (FIFA World Cup)
WC_2026_LEAGUE_ID = 1
WC_2026_SEASON    = 2026

# ── OpenMeteo (sem auth)
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"

# ── Sedes Copa 2026
VENUES = [
    {"city": "New York",       "country": "USA",    "lat": 40.8128,  "lon": -74.0742, "stadium": "MetLife Stadium"},
    {"city": "Los Angeles",    "country": "USA",    "lat": 34.0141,  "lon": -118.2879,"stadium": "SoFi Stadium"},
    {"city": "Dallas",         "country": "USA",    "lat": 32.7480,  "lon": -97.0930, "stadium": "AT&T Stadium"},
    {"city": "San Francisco",  "country": "USA",    "lat": 37.4032,  "lon": -121.9694,"stadium": "Levi's Stadium"},
    {"city": "Miami",          "country": "USA",    "lat": 25.9580,  "lon": -80.2389, "stadium": "Hard Rock Stadium"},
    {"city": "Seattle",        "country": "USA",    "lat": 47.5952,  "lon": -122.3316,"stadium": "Lumen Field"},
    {"city": "Boston",         "country": "USA",    "lat": 42.0909,  "lon": -71.2643, "stadium": "Gillette Stadium"},
    {"city": "Philadelphia",   "country": "USA",    "lat": 39.9012,  "lon": -75.1674, "stadium": "Lincoln Financial Field"},
    {"city": "Kansas City",    "country": "USA",    "lat": 39.0489,  "lon": -94.4839, "stadium": "Arrowhead Stadium"},
    {"city": "Atlanta",        "country": "USA",    "lat": 33.7554,  "lon": -84.4008, "stadium": "Mercedes-Benz Stadium"},
    {"city": "Mexico City",    "country": "MEX",    "lat": 19.3029,  "lon": -99.1505, "stadium": "Estadio Azteca"},
    {"city": "Guadalajara",    "country": "MEX",    "lat": 20.6837,  "lon": -103.3198,"stadium": "Estadio Akron"},
    {"city": "Monterrey",      "country": "MEX",    "lat": 25.6693,  "lon": -100.2558,"stadium": "Estadio BBVA"},
    {"city": "Toronto",        "country": "CAN",    "lat": 43.6332,  "lon": -79.5890, "stadium": "BMO Field"},
    {"city": "Vancouver",      "country": "CAN",    "lat": 49.2769,  "lon": -123.1125,"stadium": "BC Place"},
]

# ── Seleções Copa 2026 (grupos provisórios — atualizar após sorteio)
TEAMS = [
    # Grupo A
    {"id": "BRA", "name": "Brazil",      "group": "A", "fifa_rank": 5},
    {"id": "MEX", "name": "Mexico",      "group": "A", "fifa_rank": 11},
    {"id": "SUI", "name": "Switzerland", "group": "A", "fifa_rank": 20},
    {"id": "GHA", "name": "Ghana",       "group": "A", "fifa_rank": 60},
    # Grupo B
    {"id": "ARG", "name": "Argentina",   "group": "B", "fifa_rank": 1},
    {"id": "POL", "name": "Poland",      "group": "B", "fifa_rank": 28},
    {"id": "AUS", "name": "Australia",   "group": "B", "fifa_rank": 23},
    {"id": "SEN", "name": "Senegal",     "group": "B", "fifa_rank": 18},
    # Grupo C
    {"id": "FRA", "name": "France",      "group": "C", "fifa_rank": 2},
    {"id": "DEN", "name": "Denmark",     "group": "C", "fifa_rank": 22},
    {"id": "TUN", "name": "Tunisia",     "group": "C", "fifa_rank": 33},
    {"id": "PER", "name": "Peru",        "group": "C", "fifa_rank": 40},
    # Grupo D
    {"id": "ENG", "name": "England",     "group": "D", "fifa_rank": 4},
    {"id": "NED", "name": "Netherlands", "group": "D", "fifa_rank": 7},
    {"id": "ECU", "name": "Ecuador",     "group": "D", "fifa_rank": 44},
    {"id": "IRN", "name": "Iran",        "group": "D", "fifa_rank": 25},
    # Grupo E
    {"id": "ESP", "name": "Spain",       "group": "E", "fifa_rank": 8},
    {"id": "GER", "name": "Germany",     "group": "E", "fifa_rank": 16},
    {"id": "JPN", "name": "Japan",       "group": "E", "fifa_rank": 17},
    {"id": "CRC", "name": "Costa Rica",  "group": "E", "fifa_rank": 41},
    # Grupo F
    {"id": "POR", "name": "Portugal",    "group": "F", "fifa_rank": 6},
    {"id": "URU", "name": "Uruguay",     "group": "F", "fifa_rank": 14},
    {"id": "COR", "name": "South Korea", "group": "F", "fifa_rank": 26},
    {"id": "CIV", "name": "Ivory Coast", "group": "F", "fifa_rank": 52},
    # Grupo G
    {"id": "BEL", "name": "Belgium",     "group": "G", "fifa_rank": 3},
    {"id": "USA", "name": "USA",         "group": "G", "fifa_rank": 13},
    {"id": "MAR", "name": "Morocco",     "group": "G", "fifa_rank": 12},
    {"id": "CRO", "name": "Croatia",     "group": "G", "fifa_rank": 9},
    # Grupo H
    {"id": "PRT", "name": "Portugal",    "group": "H", "fifa_rank": 6},  # placeholder
    {"id": "SRB", "name": "Serbia",      "group": "H", "fifa_rank": 33},
    {"id": "CMR", "name": "Cameroon",    "group": "H", "fifa_rank": 55},
    {"id": "QAT", "name": "Qatar",       "group": "H", "fifa_rank": 38},
]

# ── Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
