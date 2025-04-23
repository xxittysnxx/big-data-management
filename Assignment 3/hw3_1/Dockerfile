FROM python:3.11-slim

WORKDIR /app

RUN pip install finnhub-python kafka-python

COPY producer.py .

CMD ["python", "producer.py"]
