import json
import time
import random
import os
import signal
import sys
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")

def generate_sensor_data(sensor_id):
    """Generates synthetic sensor readings."""
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 60.0), 2),
        "pressure": round(random.uniform(900.0, 1100.0), 2)
    }

def main():
    print(f"Starting producer on topic: {KAFKA_TOPIC}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    sensor_ids = ["sensor_A", "sensor_B", "sensor_C"]

    def signal_handler(sig, frame):
        print("\nStopping producer...")
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while True:
        for sensor_id in sensor_ids:
            data = generate_sensor_data(sensor_id)
            print(f"Producing to {KAFKA_TOPIC}: {data}")
            try:
                producer.send(KAFKA_TOPIC, data)
            except Exception as e:
                print(f"Error sending message: {e}")
        time.sleep(1)

if __name__ == "__main__":
    main()
