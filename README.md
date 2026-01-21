# Real-Time Sensor Data Ingestion using Kafka

## Project Overview
This project implements a Python-based real-time data ingestion microservice that consumes sensor data from an Apache Kafka topic, validates and transforms the data, and stores it in a SQLite database for further analysis.

## Architecture
- Producer generates synthetic sensor data
- Kafka streams the data
- Consumer processes messages using a consumer group
- Data is validated and stored in SQLite

## Project Structure
realtime-ingestion/
├── src/
│   ├── producer.py
│   ├── consumer.py
│   ├── processor.py
│   └── storage.py
├── data/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md

## Setup Instructions
1. Install Docker and Docker Compose
2. Run: docker-compose up --build

## Running Producer
docker-compose exec ingestion python src/producer.py

## Verifying Data
sqlite3 data/processed_data.db
SELECT * FROM sensor_readings;

## Conclusion
This project demonstrates a complete real-time data ingestion pipeline using Kafka and Docker.
