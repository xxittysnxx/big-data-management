import finnhub
import json
import time
from datetime import datetime
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

api_key = os.getenv("FINNHUB_API_KEY")
if not api_key:
    raise ValueError("FINNHUB_API_KEY is not set in environment")
finnhub_client = finnhub.Client(api_key=api_key)

# Avoid producer.py load before Kafka Server
def get_producer():
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("KafkaProducer connected")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {2 ** i} sec...")
            time.sleep(2 ** i)
    raise Exception("Kafka is not available after multiple retries")

producer = get_producer()

def fetch_news():
    try:
        # Pull the latest news from finnhub
        news = finnhub_client.general_news('general', min_id=0)
        return news
    except Exception as e:
        print(f"Error fetching news: {e}")
        return []

while True:
    news_items = fetch_news()
    for item in news_items:
        message = {
            "headline": item.get("headline", ""),
            "summary": item.get("summary", ""),
            "datetime": datetime.fromtimestamp(item.get("datetime", 0)).isoformat()
        }
        producer.send('topic1', message)
    print(f"Sent {len(news_items)} news items to Kafka")

    # Fetch every 30 seconds
    time.sleep(30)