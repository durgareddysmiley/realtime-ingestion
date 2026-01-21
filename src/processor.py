import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles validation and transformation of sensor data."""

    def process(self, raw_message: bytes) -> dict:
        """
        Processes raw bytes from Kafka into a validated dictionary.
        
        Args:
            raw_message: The raw bytes received from Kafka.
            
        Returns:
            dict: The validated and transformed data.
            
        Raises:
            ValueError: If validation or transformation fails.
        """
        # Step 1: Convert bytes → JSON
        try:
            decoded_msg = raw_message.decode("utf-8")
            data = json.loads(decoded_msg)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")

        # Step 2: Check required fields
        required_fields = [
            "sensor_id",
            "timestamp",
            "temperature",
            "humidity",
            "pressure"
        ]

        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

        # Step 3: Validate sensor_id
        if not isinstance(data["sensor_id"], str) or not data["sensor_id"].strip():
            raise ValueError("sensor_id must be a non-empty string")

        # Step 4: Validate and convert timestamp
        try:
            # datetime.fromisoformat handles ISO8601
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid timestamp format: {e}")

        # Step 5: Validate numeric fields
        for key in ["temperature", "humidity", "pressure"]:
            if not isinstance(data[key], (int, float)):
                raise ValueError(f"Field '{key}' must be a number (int or float), got {type(data[key]).__name__}")

        return data
