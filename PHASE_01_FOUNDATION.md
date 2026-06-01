# Fase 01 — Foundation

**Status:** ✅ Completa  
**Data:** 2026-05  
**Tempo estimado:** ~10h | **Tempo real:** a preencher

---

## Objetivo

Montar a infraestrutura base do projeto: Kafka local, producers Python, seeds de dados estáticos e scaffold do Databricks Asset Bundle.

---

## Arquivos Gerados

| Arquivo | Descrição |
|---|---|
| `CONTEXT.md` | Master context — ler antes de cada sessão |
| `docker-compose.yml` | Kafka 7.6 + Zookeeper + Kafka UI + Schema Registry |
| `requirements.txt` | Deps Python dos producers |
| `producers/config.py` | Configuração central (Kafka, API keys, VENUES, TEAMS) |
| `producers/match_events_producer.py` | Producer de eventos de partida (simulate + live) |
| `producers/odds_producer.py` | Simulador de movimento de odds (1X2, O/U) |
| `producers/weather_producer.py` | Coleta clima das sedes via OpenMeteo |
| `producers/static_loader.py` | Gera seeds JSON (rankings, história, elencos) |
| `scripts/setup_kafka_topics.sh` | Cria os 4 topics no broker local |
| `scripts/upload_to_dbfs.sh` | Upload dos seeds para DBFS via Databricks CLI |
| `bundle/databricks.yml` | DAB scaffold (para workspace pago futuro) |
| `bundle/resources/jobs.yml` | Definição dos 6 jobs (referência) |

---

## Pré-requisitos

```bash
# 1. Docker Desktop instalado e rodando
docker --version  # >= 24.x

# 2. Python 3.10+
python --version

# 3. Dependências dos producers
pip install -r requirements.txt

# 4. (Opcional) Databricks CLI para upload ao DBFS
pip install databricks-cli
databricks configure --token
```

---

## Execução — Passo a Passo

### 1. Subir o Kafka local

```bash
# Na raiz do projeto
docker-compose up -d

# Verificar se está saudável
docker-compose ps

# Kafka UI disponível em:
# http://localhost:8080
```

### 2. Criar os topics

```bash
bash scripts/setup_kafka_topics.sh
```

Topics criados:
- `wc.match.events` — 3 partições, 7 dias
- `wc.odds.live` — 3 partições, 2 dias  
- `wc.weather` — 1 partição, 1 dia
- `wc.lineups` — 1 partição, 7 dias

### 3. Gerar os seeds estáticos

```bash
cd producers
python static_loader.py
# → salva em data/seeds/
```

### 4. Upload para DBFS (requer Databricks CLI)

```bash
bash scripts/upload_to_dbfs.sh
# → envia data/seeds/*.json para dbfs:/FileStore/wc-platform/seeds/
```

### 5. Testar os producers

```bash
cd producers

# Simular uma partida completa BRA x MEX (delay realista)
python match_events_producer.py --mode simulate --match-id WC2026_BRA_MEX --home BRA --away MEX

# Simular odds para a mesma partida (em outro terminal)
python odds_producer.py --match-id WC2026_BRA_MEX --home-rank 5 --away-rank 11

# Coletar clima das 15 sedes (uma vez)
python weather_producer.py
```

Verificar no Kafka UI (http://localhost:8080) se as mensagens chegaram.

---

## Decisões Tomadas

| Decisão | Motivo |
|---|---|
| Kafka via Docker (não Confluent Cloud) | Custo zero, sem limitações de free tier |
| Producers em modo `simulate` como padrão | Portfólio não depende de API key ativa |
| `delay_ms=200` no match_events_producer | Simula ~1 evento a cada 200ms → partida em ~30s |
| `static_loader.py` gera seeds sintéticos | FIFA rankings e elencos curados manualmente |
| DAB scaffold sem execução | Community Edition sem Jobs API; documentado para replicação |

---

## Problemas Conhecidos

- Community Edition não executa DAB jobs — execução é manual via notebook
- `weather_producer.py` depende de conectividade com `api.open-meteo.com`
- API-Football requer chave paga para modo `--mode live`

---

## Próxima Fase

**Fase 02 — Bronze**: Notebooks DLT para ingestão dos eventos Kafka → Delta Bronze, Auto Loader para StatsBomb histórico.

Arquivo: `docs/PHASE_02_BRONZE.md`
