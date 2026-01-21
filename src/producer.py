import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer


def generate_sensor_data(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 60.0), 2),
        "pressure": round(random.uniform(900.0, 1100.0), 2)
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sensor_ids = ["sensor_A", "sensor_B", "sensor_C"]

    while True:
        for sensor_id in sensor_ids:
            data = generate_sensor_data(sensor_id)
            print("Producing:", data)
            producer.send("sensor_data", data)
        time.sleep(1)


if __name__ == "__main__":
    main()
