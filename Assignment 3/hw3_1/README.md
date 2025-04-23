# Real-time Named Entity Recognition from Finnhub News with Kafka, Spark, and ELK Stack

This project builds a real-time data pipeline that extracts **Named Entities** (like people, organizations, and locations) from live financial news headlines from [Finnhub](https://finnhub.io), counts their mentions using **Spark Structured Streaming**, and visualizes the top entities with **Kibana**.

---

## Tech Stack

- **Finnhub API** – Real-time financial news source
- **Apache Kafka** – Stream backbone
- **Apache Spark (Structured Streaming)** – Named Entity Recognition + counting
- **Logstash** – Stream ingestor into Elasticsearch
- **Elasticsearch** – Data indexer
- **Kibana** – Data visualization

---

## Project Structure

```
.
├── docker-compose.yml
├── Dockerfile
├── .env
├── requirements.txt
├── producer.py # Finnhub to Kafka (topic1)
├── spark.py # Kafka topic1 -> NER -> Kafka topic2
├── logstash.conf # Kafka topic2 -> Elasticsearch
└── README.md
```

---

## Setup Instructions

### 1. Prerequisites

- Docker + Docker Compose

### 2. Clone the repo and set your Finnhub API Key

- Create a .env file (this will be used by Docker Compose):

```
# .env
FINNHUB_API_KEY=your_finnhub_api_key_here
```

### 3. Start all services

```
docker-compose up --build
```

- **Services launched**:
- Kafka + Zookeeper
- Spark
- Producer (streaming Finnhub news to Kafka topic1)
- Logstash (Kafka topic2 → Elasticsearch)
- Elasticsearch + Kibana

---

## How the Pipeline Works

```
[Finnhub API] --> [Producer.py] --> Kafka topic1
                                  |
                                  v
                   [Spark Structured Streaming: NER]
                                  |
                                  v
                             Kafka topic2
                                  |
                                  v
                     [Logstash] --> Elasticsearch
                                  |
                                  v
                              [Kibana]
```

---

## Visualizing Top Named Entities

- Visit: http://localhost:5601
- Go to: Stack Management → Index Patterns
- Create a new index pattern:`finnhub_named_entities`
- Go to: Visualize → Create Visualization
