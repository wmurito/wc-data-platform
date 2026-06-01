"""
WorldCup Data Platform
Producer: odds_producer.py
===========================
Simula movimento de odds (1X2, Over/Under, BTTS) em tempo real
para uma partida da Copa do Mundo.

O mercado é simulado com:
  - Ponto de partida: odds baseadas no FIFA ranking dos times
  - Drift: cada gol move as odds ~15-25% na direção do resultado
  - Ruído gaussiano: ±2% por minuto (comportamento de mercado real)
  - Spike: cartão vermelho move odds em ~10%

Uso:
  python odds_producer.py --match-id WC2026_BRA_MEX --home-rank 5 --away-rank 11

Schema (JSON):
{
  "snapshot_id":  "uuid4",
  "match_id":     "WC2026_BRA_MEX",
  "minute":       34,
  "market":       "1X2",           # ou "over_under_2_5", "btts"
  "odd_home":     1.85,
  "odd_draw":     3.40,
  "odd_away":     4.10,
  "implied_prob_home": 0.541,
  "implied_prob_draw": 0.294,
  "implied_prob_away": 0.244,
  "margin":       0.079,           # overround da bookmaker (realismo)
  "trigger":      "goal_home",     # o que causou a variação
  "ingested_at":  "2026-06-15T20:34:12Z"
}
"""

import argparse
import json
import time
import uuid
import math
import random
from datetime import datetime, timezone
from loguru import logger
from confluent_kafka import Producer
from config import KAFKA_PRODUCER_CONFIG, TOPICS


def rank_to_base_odds(home_rank: int, away_rank: int) -> tuple[float, float, float]:
    """
    Converte FIFA rankings em odds iniciais (1X2).
    Fórmula empírica baseada em dados históricos de Copa do Mundo.
    """
    rank_diff = away_rank - home_rank  # positivo = home é favorito

    # Probabilidade base do home win usando sigmoid
    p_home = 1 / (1 + math.exp(-rank_diff * 0.04))
    p_draw = 0.26 - abs(rank_diff) * 0.003  # empate diminui com diferença grande
    p_draw = max(0.18, min(0.30, p_draw))
    p_away = 1 - p_home - p_draw

    # Aplicar margin de bookmaker (~7%)
    margin = 1.07
    odd_home = round((margin / p_home), 2)
    odd_draw = round((margin / p_draw), 2)
    odd_away = round((margin / p_away), 2)

    return odd_home, odd_draw, odd_away


def implied_probs(odd_h: float, odd_d: float, odd_a: float) -> tuple[float, float, float]:
    total = 1/odd_h + 1/odd_d + 1/odd_a
    return round(1/odd_h/total, 4), round(1/odd_d/total, 4), round(1/odd_a/total, 4)


def apply_event_shock(odd_h: float, odd_d: float, odd_a: float,
                      trigger: str) -> tuple[float, float, float]:
    """Aplica choque nas odds baseado em evento de jogo."""
    shocks = {
        "goal_home":   (-0.20, +0.10, +0.30),  # home scores: home fica favorito
        "goal_away":   (+0.30, +0.10, -0.20),
        "red_home":    (+0.15, +0.05, -0.18),   # home perde jogador
        "red_away":    (-0.18, +0.05, +0.15),
        "var_reversed":( 0.05, -0.02, -0.03),   # confusão geral
    }
    sh, sd, sa = shocks.get(trigger, (0, 0, 0))
    return (
        max(1.01, round(odd_h * (1 + sh + random.gauss(0, 0.01)), 2)),
        max(1.01, round(odd_d * (1 + sd + random.gauss(0, 0.01)), 2)),
        max(1.01, round(odd_a * (1 + sa + random.gauss(0, 0.01)), 2)),
    )


def simulate_match_odds(producer: Producer, match_id: str,
                        home_rank: int, away_rank: int,
                        delay_ms: int = 150):
    """Simula odds para 90 minutos de partida."""

    odd_h, odd_d, odd_a = rank_to_base_odds(home_rank, away_rank)
    score = [0, 0]

    logger.info(f"[ODDS] Starting simulation for {match_id}")
    logger.info(f"[ODDS] Initial odds → H:{odd_h} D:{odd_d} A:{odd_a}")

    # Sortear eventos de gol (consistente com match_events, mas independente)
    n_goals_home = max(0, int(random.gauss(1.3, 0.9)))
    n_goals_away = max(0, int(random.gauss(1.3, 0.9)))
    goal_minutes_home = sorted(random.sample(range(5, 91), min(n_goals_home, 85)))
    goal_minutes_away = sorted(random.sample(range(5, 91), min(n_goals_away, 85)))
    red_minute = random.randint(55, 85) if random.random() < 0.10 else None
    red_team   = random.choice(["home", "away"]) if red_minute else None

    for minute in range(0, 92):
        trigger = "tick"

        # Choque por gol
        if minute in goal_minutes_home:
            score[0] += 1
            trigger = "goal_home"
            odd_h, odd_d, odd_a = apply_event_shock(odd_h, odd_d, odd_a, trigger)
        elif minute in goal_minutes_away:
            score[1] += 1
            trigger = "goal_away"
            odd_h, odd_d, odd_a = apply_event_shock(odd_h, odd_d, odd_a, trigger)

        # Choque por cartão vermelho
        if minute == red_minute:
            trigger = f"red_{red_team}"
            odd_h, odd_d, odd_a = apply_event_shock(odd_h, odd_d, odd_a, trigger)

        # Ruído gaussiano de mercado (±1.5%)
        noise = random.gauss(0, 0.008)
        odd_h = max(1.01, round(odd_h * (1 + noise), 2))
        odd_d = max(1.01, round(odd_d * (1 + noise * 0.5), 2))
        odd_a = max(1.01, round(odd_a * (1 - noise), 2))

        ip_h, ip_d, ip_a = implied_probs(odd_h, odd_d, odd_a)
        margin = round(1/odd_h + 1/odd_d + 1/odd_a - 1, 4)

        # Over/Under 2.5 (simplificado)
        goals_so_far = score[0] + score[1]
        remaining    = 90 - minute
        p_over = min(0.95, max(0.05,
            0.45 + goals_so_far * 0.18 - remaining * 0.003
        ))
        odd_over  = round(1.05 / p_over, 2)
        odd_under = round(1.05 / (1 - p_over), 2)

        snapshot = {
            "snapshot_id":       str(uuid.uuid4()),
            "match_id":          match_id,
            "minute":            minute,
            "score_home":        score[0],
            "score_away":        score[1],
            "market_1x2": {
                "odd_home":          odd_h,
                "odd_draw":          odd_d,
                "odd_away":          odd_a,
                "implied_prob_home": ip_h,
                "implied_prob_draw": ip_d,
                "implied_prob_away": ip_a,
                "margin":            margin,
            },
            "market_ou25": {
                "odd_over":  odd_over,
                "odd_under": odd_under,
                "p_over":    round(p_over, 4),
            },
            "trigger":     trigger,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        payload = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic=TOPICS["odds"],
            key=match_id.encode("utf-8"),
            value=payload,
        )
        producer.poll(0)
        time.sleep(delay_ms / 1000)

    producer.flush()
    logger.success(f"[ODDS] {match_id} — 92 snapshots published. Final: {score[0]}-{score[1]} ✓")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WorldCup odds producer")
    parser.add_argument("--match-id",   default="WC2026_BRA_MEX")
    parser.add_argument("--home-rank",  type=int, default=5)
    parser.add_argument("--away-rank",  type=int, default=11)
    parser.add_argument("--delay-ms",   type=int, default=150)
    args = parser.parse_args()

    producer = Producer(KAFKA_PRODUCER_CONFIG)
    simulate_match_odds(producer, args.match_id, args.home_rank,
                        args.away_rank, args.delay_ms)
