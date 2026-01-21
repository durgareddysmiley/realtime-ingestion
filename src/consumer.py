import json
import logging
import os
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from processor import DataProcessor
from storage import DataStorage
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "sensor_data_dlq")
GROUP_ID = os.getenv("GROUP_ID", "sensor-consumer-group")
DB_PATH = os.getenv("DB_PATH", "data/processed_data.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self):
        self.running = True
        self.processor = DataProcessor()
        self.storage = DataStorage(db_path=DB_PATH)
        
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            
            # Producer for DLQ
            self.dlq_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Failed to initialize Kafka connections: {e}")
            sys.exit(1)

    def handle_exit(self, sig, frame):
        logger.info("Shutdown signal received. Closing connections...")
        self.running = False
        self.consumer.close()
        self.dlq_producer.close()
        self.storage.close()
        sys.exit(0)

    def run(self):
        logger.info(f"Starting ingestion service. Consuming from {KAFKA_TOPIC}")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

        while self.running:
            # poll() allows for more control and graceful shutdown than direct iteration
            msg_pack = self.consumer.poll(timeout_ms=1000)
            
            for tp, messages in msg_pack.items():
                for message in messages:
                    try:
                        processed_data = self.processor.process(message.value)
                        self.storage.insert(processed_data)
                        logger.info(f"Processed and stored data for sensor: {processed_data['sensor_id']}")
                    except ValueError as e:
                        logger.warning(f"Validation failed, sending to DLQ: {e}")
                        self.send_to_dlq(message.value, str(e))
                    except Exception as e:
                        logger.error(f"Unexpected error processing message: {e}")

    def send_to_dlq(self, raw_value, error_msg):
        """Sends faulty messages to a Dead Letter Queue topic."""
        try:
            dlq_message = {
                "original_message": raw_value.decode("utf-8", errors="replace"),
                "error": error_msg,
                "timestamp": datetime.now().isoformat()
            }
            
            self.dlq_producer.send(KAFKA_DLQ_TOPIC, dlq_message)
            logger.info(f"Faulty message sent to topic: {KAFKA_DLQ_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

if __name__ == "__main__":
    service = IngestionService()
    service.run()
