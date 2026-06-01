#!/bin/bash
# WorldCup Data Platform — Setup dos Kafka Topics
# Rodar APÓS: docker-compose up -d
# Aguarda o broker estar saudável antes de criar os topics.

set -e

BROKER="localhost:9092"
CONTAINER="wc_kafka"

echo "⏳ Aguardando Kafka broker em $BROKER..."
until docker exec $CONTAINER kafka-broker-api-versions --bootstrap-server $BROKER &>/dev/null; do
  sleep 3
  echo "   ainda aguardando..."
done
echo "✅ Kafka broker pronto."

create_topic() {
  local TOPIC=$1
  local PARTITIONS=$2
  local RETENTION_MS=$3

  docker exec $CONTAINER kafka-topics \
    --bootstrap-server $BROKER \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    --config cleanup.policy=delete

  echo "✅ Topic criado: $TOPIC (partições=$PARTITIONS, retention=${RETENTION_MS}ms)"
}

echo ""
echo "📦 Criando topics..."

# wc.match.events — eventos de partida (retenção 7 dias)
create_topic "wc.match.events" 3 604800000

# wc.odds.live — snapshots de odds (retenção 2 dias)
create_topic "wc.odds.live" 3 172800000

# wc.weather — condições climáticas (retenção 1 dia)
create_topic "wc.weather" 1 86400000

# wc.lineups — escalações (retenção 7 dias)
create_topic "wc.lineups" 1 604800000

echo ""
echo "📋 Topics criados:"
docker exec $CONTAINER kafka-topics --bootstrap-server $BROKER --list

echo ""
echo "🎉 Setup completo! Kafka UI disponível em: http://localhost:8080"
