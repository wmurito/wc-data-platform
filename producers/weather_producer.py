"""
WorldCup Data Platform
Producer: weather_producer.py
==============================
Coleta condições climáticas das sedes da Copa 2026 via OpenMeteo (free, sem auth)
e publica no topic `wc.weather`.

Uso:
  python weather_producer.py                     # coleta todas as sedes, uma vez
  python weather_producer.py --loop --interval 3600  # loop a cada hora

Schema (JSON):
{
  "weather_id":        "uuid4",
  "venue":             "MetLife Stadium",
  "city":              "New York",
  "country":           "USA",
  "lat":               40.8128,
  "lon":               -74.0742,
  "temperature_c":     28.4,
  "humidity_pct":      72,
  "wind_speed_kmh":    14.2,
  "wind_direction":    "SW",
  "precipitation_mm":  0.0,
  "weather_code":      0,            # WMO code (0=clear, 61=rain, etc.)
  "weather_desc":      "Clear sky",
  "collected_at":      "2026-06-15T20:00:00Z"
}
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
import requests
from loguru import logger
from confluent_kafka import Producer
from config import KAFKA_PRODUCER_CONFIG, TOPICS, VENUES

# WMO Weather codes → descrição
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm + hail", 99: "Thunderstorm + heavy hail",
}

WIND_DIRS = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]

def degrees_to_cardinal(deg: float) -> str:
    idx = round(deg / 22.5) % 16
    return WIND_DIRS[idx]


def fetch_weather(venue: dict) -> dict | None:
    """Chama OpenMeteo API para o venue dado."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":               venue["lat"],
        "longitude":              venue["lon"],
        "current":                "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
        "wind_speed_unit":        "kmh",
        "timezone":               "auto",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        cur  = data.get("current", {})
        wcode = cur.get("weather_code", 0)

        return {
            "weather_id":       str(uuid.uuid4()),
            "venue":            venue["stadium"],
            "city":             venue["city"],
            "country":          venue["country"],
            "lat":              venue["lat"],
            "lon":              venue["lon"],
            "temperature_c":    cur.get("temperature_2m"),
            "humidity_pct":     cur.get("relative_humidity_2m"),
            "wind_speed_kmh":   cur.get("wind_speed_10m"),
            "wind_direction":   degrees_to_cardinal(cur.get("wind_direction_10m", 0)),
            "precipitation_mm": cur.get("precipitation", 0.0),
            "weather_code":     wcode,
            "weather_desc":     WMO_CODES.get(wcode, "Unknown"),
            "collected_at":     datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"Failed to fetch weather for {venue['city']}: {e}")
        return None


def publish_all(producer: Producer):
    """Coleta e publica weather de todas as sedes."""
    published = 0
    for venue in VENUES:
        record = fetch_weather(venue)
        if record:
            payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
            producer.produce(
                topic=TOPICS["weather"],
                key=venue["city"].encode("utf-8"),
                value=payload,
            )
            producer.poll(0)
            logger.info(f"[WEATHER] {venue['city']}: {record['temperature_c']}°C, "
                        f"{record['weather_desc']}, wind {record['wind_speed_kmh']} km/h")
            published += 1
            time.sleep(0.3)  # rate limit gentil

    producer.flush()
    logger.success(f"[WEATHER] {published}/{len(VENUES)} venues published ✓")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WorldCup weather producer")
    parser.add_argument("--loop",     action="store_true", help="Roda em loop contínuo")
    parser.add_argument("--interval", type=int, default=3600, help="Intervalo em segundos (padrão: 1h)")
    args = parser.parse_args()

    producer = Producer(KAFKA_PRODUCER_CONFIG)

    if args.loop:
        logger.info(f"[WEATHER] Loop mode — collecting every {args.interval}s")
        while True:
            publish_all(producer)
            logger.info(f"[WEATHER] Sleeping {args.interval}s...")
            time.sleep(args.interval)
    else:
        publish_all(producer)
