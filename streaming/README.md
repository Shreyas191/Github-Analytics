# Streaming Pipeline — Shreyas Kaldate (sk12898)

Real-time GitHub event ingestion via Kafka + PySpark Structured Streaming.

## Files
| File | Ticket | Description |
|---|---|---|
| `stream01_pat_rotator.py` | STREAM-01 | GitHub PAT round-robin rotator |
| `stream02_create_topics.py` | STREAM-02 | Kafka topic setup (run once) |
| `stream03_producer.py` | STREAM-03 | GitHub Events API → Kafka producer |
| `requirements.txt` | — | Python dependencies |

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Kafka (if team Docker Compose isn't up yet)
```bash
docker run -d \
  --name kafka-local \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  apache/kafka:3.7.0
```

### 3. Set PATs as environment variables
```bash
cp .env.example .env
# Edit .env and fill in real tokens
```

Or export directly:
```bash
export PAT_SHREYAS=ghp_...
export PAT_HARIHARAN=ghp_...
export PAT_TANUSHA=ghp_...
export PAT_VIKRAM=ghp_...
```

### 4. Create Kafka topics (run once)
```bash
python stream02_create_topics.py
```

### 5. Start the producer
```bash
python stream03_producer.py
```

## Kafka Topics
| Topic | Event Type | Partitions | Retention |
|---|---|---|---|
| push-events | PushEvent | 4 | 48h |
| watch-events | WatchEvent (stars) | 4 | 48h |
| fork-events | ForkEvent | 4 | 48h |
| pr-events | PullRequestEvent | 4 | 48h |
| issue-events | IssuesEvent | 4 | 48h |
| create-events | CreateEvent | 4 | 48h |
| pending-repos | ML scoring queue | 4 | 48h |

## Week Plan
- **Week 1**: STREAM-01, 02, 03 — Kafka topics up, producer polling live
- **Week 2**: STREAM-04, 05 — Schema parity test, PySpark Structured Streaming
- **Week 3**: STREAM-06, 07 — 48h ML scoring queue, throughput logging
