"""
WorldCup Data Platform
Producer: match_events_producer.py
===================================
Publica eventos de partida no topic `wc.match.events`.

Modos de operação:
  --mode simulate   (padrão) Gera partida simulada completa (90 min em segundos)
  --mode live       Polling da API-Football (requer API key em .env)

Uso:
  python match_events_producer.py --mode simulate --match-id WC2026_BRA_MEX
  python match_events_producer.py --mode live --match-id 12345

Schema do evento (JSON):
{
  "event_id":    "uuid4",
  "match_id":    "WC2026_BRA_MEX",
  "event_type":  "goal|yellow_card|red_card|substitution|kickoff|halftime|fulltime|var_review|penalty",
  "minute":      45,
  "extra_time":  2,
  "team_id":     "BRA",
  "player_id":   "neymar_jr",
  "player_name": "Neymar Jr.",
  "detail":      "Left Foot Shot",
  "assist_player_id": null,
  "score_home":  1,
  "score_away":  0,
  "venue":       "MetLife Stadium",
  "ingested_at": "2026-06-15T20:34:12Z"
}
"""

import argparse
import json
import time
import uuid
import random
from datetime import datetime, timezone
from loguru import logger
from confluent_kafka import Producer
from config import KAFKA_PRODUCER_CONFIG, TOPICS, TEAMS, VENUES

# ── Delivery callback
def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered → {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


# ── Gerador de partida simulada
class MatchSimulator:
    """
    Simula os eventos de uma partida de futebol de 90 minutos.
    Baseado em distribuições estatísticas reais da Copa do Mundo:
      - Média de 2.6 gols por jogo
      - Média de 3.6 cartões amarelos por jogo
      - ~28% de jogos com substituição forçada por lesão
    """

    EVENT_TYPES_WEIGHTED = [
        ("goal",         4),
        ("yellow_card",  7),
        ("red_card",     1),
        ("substitution", 9),
        ("var_review",   3),
        ("foul",        15),   # foul não vai para o topic (ruído), apenas para realismo
    ]

    PLAYERS = {
        "BRA": [
            ("alisson_becker",   "Alisson Becker"),
            ("marquinhos",       "Marquinhos"),
            ("thiago_silva",     "Thiago Silva"),
            ("danilo",           "Danilo"),
            ("casemiro",         "Casemiro"),
            ("lucas_paqueta",    "Lucas Paquetá"),
            ("rodrygo",          "Rodrygo"),
            ("vinicius_jr",      "Vinícius Jr."),
            ("richarlison",      "Richarlison"),
            ("raphinha",         "Raphinha"),
            ("endrick",          "Endrick"),
        ],
        "MEX": [
            ("guillermo_ochoa",  "Guillermo Ochoa"),
            ("hector_moreno",    "Héctor Moreno"),
            ("hirving_lozano",   "Hirving Lozano"),
            ("raul_jimenez",     "Raúl Jiménez"),
            ("andres_guardado",  "Andrés Guardado"),
            ("carlos_salcedo",   "Carlos Salcedo"),
            ("henry_martin",     "Henry Martín"),
        ],
        # Fallback genérico para outros times
        "_default": [
            ("player_01", "Player 01"), ("player_02", "Player 02"),
            ("player_03", "Player 03"), ("player_04", "Player 04"),
            ("player_05", "Player 05"), ("player_06", "Player 06"),
        ]
    }

    def __init__(self, match_id: str, home_team: str, away_team: str, venue: str):
        self.match_id   = match_id
        self.home_team  = home_team
        self.away_team  = away_team
        self.venue      = venue
        self.score_home = 0
        self.score_away = 0
        self.events     = []

    def _get_players(self, team_id: str):
        return self.PLAYERS.get(team_id, self.PLAYERS["_default"])

    def _random_player(self, team_id: str):
        players = self._get_players(team_id)
        pid, pname = random.choice(players)
        return pid, pname

    def _base_event(self, minute: int, extra: int, event_type: str, team_id: str) -> dict:
        pid, pname = self._random_player(team_id)
        return {
            "event_id":          str(uuid.uuid4()),
            "match_id":          self.match_id,
            "event_type":        event_type,
            "minute":            minute,
            "extra_time":        extra,
            "team_id":           team_id,
            "player_id":         pid,
            "player_name":       pname,
            "detail":            "",
            "assist_player_id":  None,
            "score_home":        self.score_home,
            "score_away":        self.score_away,
            "venue":             self.venue,
            "ingested_at":       datetime.now(timezone.utc).isoformat(),
        }

    def generate(self) -> list[dict]:
        events = []

        # Kickoff
        ko = self._base_event(0, 0, "kickoff", self.home_team)
        ko["detail"] = "Match started"
        events.append(ko)

        # Gera eventos aleatórios por minuto (simplificado)
        n_goals   = max(0, int(random.gauss(2.6, 1.2)))
        n_yellows = max(0, int(random.gauss(3.6, 1.5)))
        n_reds    = 1 if random.random() < 0.12 else 0
        n_subs    = random.randint(4, 6)  # cada time pode fazer até 5

        # Distribute gols por minuto
        goal_minutes = sorted(random.sample(range(1, 91), min(n_goals, 89)))
        yellow_minutes = sorted(random.sample(range(5, 91), min(n_yellows, 80)))
        red_minutes    = sorted(random.sample(range(30, 91), n_reds))
        sub_minutes    = sorted(random.sample(range(46, 89), min(n_subs, 43)))

        for minute in goal_minutes:
            team = random.choice([self.home_team, self.away_team])
            ev = self._base_event(minute, 0, "goal", team)
            assist_pid, assist_pname = self._random_player(team)
            ev["assist_player_id"] = assist_pid
            ev["detail"] = random.choice(["Right Foot", "Left Foot", "Header", "Penalty"])
            if team == self.home_team:
                self.score_home += 1
            else:
                self.score_away += 1
            ev["score_home"] = self.score_home
            ev["score_away"] = self.score_away

            # ~15% chance de VAR review antes do gol
            if random.random() < 0.15:
                var_ev = self._base_event(minute - 1, 0, "var_review", team)
                var_ev["detail"] = "Offside check"
                events.append(var_ev)

            events.append(ev)

        for minute in yellow_minutes:
            team = random.choice([self.home_team, self.away_team])
            ev = self._base_event(minute, 0, "yellow_card", team)
            ev["detail"] = random.choice(["Foul", "Time wasting", "Unsporting behaviour"])
            events.append(ev)

        for minute in red_minutes:
            team = random.choice([self.home_team, self.away_team])
            ev = self._base_event(minute, 0, "red_card", team)
            ev["detail"] = random.choice(["Violent conduct", "Second yellow", "Denial of goal"])
            events.append(ev)

        for minute in sub_minutes:
            team = random.choice([self.home_team, self.away_team])
            ev = self._base_event(minute, 0, "substitution", team)
            out_pid, out_pname = self._random_player(team)
            ev["detail"] = f"Out: {out_pname}"
            events.append(ev)

        # Halftime
        ht = self._base_event(45, random.randint(0, 5), "halftime", self.home_team)
        ht["detail"] = f"HT: {self.score_home}-{self.score_away}"
        events.append(ht)

        # Fulltime
        ft = self._base_event(90, random.randint(0, 7), "fulltime", self.home_team)
        ft["score_home"] = self.score_home
        ft["score_away"] = self.score_away
        ft["detail"] = f"FT: {self.score_home}-{self.score_away}"
        events.append(ft)

        # Ordenar por minuto
        events.sort(key=lambda e: (e["minute"], e["extra_time"]))
        return events


# ── Producer principal
def run_simulate(producer: Producer, match_id: str, home: str, away: str,
                 venue: str, delay_ms: int = 200):
    """Simula uma partida completa e publica todos os eventos no Kafka."""
    logger.info(f"[SIMULATE] {match_id}: {home} vs {away} @ {venue}")

    sim = MatchSimulator(match_id, home, away, venue)
    events = sim.generate()

    logger.info(f"Generated {len(events)} events for {match_id}")

    for ev in events:
        payload = json.dumps(ev, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic=TOPICS["match_events"],
            key=ev["match_id"].encode("utf-8"),
            value=payload,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        time.sleep(delay_ms / 1000)

    producer.flush()
    logger.success(f"[SIMULATE] {match_id} — {len(events)} events published ✓")


def run_live(producer: Producer, match_id: str, api_key: str):
    """Polling da API-Football para partida ao vivo."""
    import requests
    from config import API_FOOTBALL_BASE

    if not api_key:
        logger.error("API_FOOTBALL_KEY não configurada. Use --mode simulate ou defina a key no .env")
        return

    headers = {
        "X-RapidAPI-Key":  api_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
    }

    logger.info(f"[LIVE] Polling match {match_id}...")
    while True:
        try:
            resp = requests.get(
                f"{API_FOOTBALL_BASE}/fixtures/events",
                headers=headers,
                params={"fixture": match_id},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            for ev in data.get("response", []):
                normalized = {
                    "event_id":    str(uuid.uuid4()),
                    "match_id":    str(match_id),
                    "event_type":  ev.get("type", "unknown").lower().replace(" ", "_"),
                    "minute":      ev.get("time", {}).get("elapsed", 0),
                    "extra_time":  ev.get("time", {}).get("extra", 0) or 0,
                    "team_id":     ev.get("team", {}).get("name", ""),
                    "player_id":   str(ev.get("player", {}).get("id", "")),
                    "player_name": ev.get("player", {}).get("name", ""),
                    "detail":      ev.get("detail", ""),
                    "assist_player_id": str(ev.get("assist", {}).get("id", "")) or None,
                    "score_home":  None,
                    "score_away":  None,
                    "venue":       "",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                payload = json.dumps(normalized, ensure_ascii=False).encode("utf-8")
                producer.produce(
                    topic=TOPICS["match_events"],
                    key=str(match_id).encode("utf-8"),
                    value=payload,
                    on_delivery=delivery_report,
                )

            producer.poll(0)
            producer.flush()
            logger.info(f"[LIVE] Polled {len(data.get('response', []))} events. Sleeping 30s...")
            time.sleep(30)

        except Exception as e:
            logger.error(f"[LIVE] Error: {e}. Retrying in 60s...")
            time.sleep(60)


# ── CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WorldCup match events producer")
    parser.add_argument("--mode",     choices=["simulate", "live"], default="simulate")
    parser.add_argument("--match-id", default="WC2026_BRA_MEX")
    parser.add_argument("--home",     default="BRA")
    parser.add_argument("--away",     default="MEX")
    parser.add_argument("--venue",    default="MetLife Stadium")
    parser.add_argument("--delay-ms", type=int, default=200,
                        help="Delay entre eventos em modo simulate (ms)")
    args = parser.parse_args()

    producer = Producer(KAFKA_PRODUCER_CONFIG)

    if args.mode == "simulate":
        run_simulate(producer, args.match_id, args.home, args.away,
                     args.venue, args.delay_ms)
    else:
        from config import API_FOOTBALL_KEY
        run_live(producer, args.match_id, API_FOOTBALL_KEY)
