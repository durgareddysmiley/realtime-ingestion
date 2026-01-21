import json
from datetime import datetime


class DataProcessor:
    def process(self, raw_message: bytes):
        # Step 1: Convert bytes → JSON
        try:
            data = json.loads(raw_message.decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON data")

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
                raise ValueError(f"Missing field: {field}")

        # Step 3: Validate sensor_id
        if not isinstance(data["sensor_id"], str) or not data["sensor_id"]:
            raise ValueError("sensor_id must be a non-empty string")

        # Step 4: Validate and convert timestamp
        try:
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        except ValueError:
            raise ValueError("Invalid timestamp format")

        # Step 5: Validate numeric fields
        for key in ["temperature", "humidity", "pressure"]:
            if not isinstance(data[key], (int, float)):
                raise ValueError(f"{key} must be a number")

        return data
