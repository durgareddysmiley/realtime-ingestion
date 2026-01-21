import logging
from kafka import KafkaConsumer
from processor import DataProcessor
from storage import DataStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    consumer = KafkaConsumer(
        "sensor_data",
        bootstrap_servers="kafka:9092",
        group_id="sensor-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    processor = DataProcessor()
    storage = DataStorage()

    for message in consumer:
        try:
            processed_data = processor.process(message.value)
            storage.insert(processed_data)
            logging.info("Data stored successfully")
        except Exception as e:
            logging.error(f"Failed to process message: {e}")


if __name__ == "__main__":
    main()
