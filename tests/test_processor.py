import json
import pytest
from src.processor import DataProcessor
from datetime import datetime


def test_valid_message():
    processor = DataProcessor()
    msg = json.dumps({
        "sensor_id": "sensor_1",
        "timestamp": datetime.now().isoformat(),
        "temperature": 25.5,
        "humidity": 50.0,
        "pressure": 1000.0
    }).encode("utf-8")

    result = processor.process(msg)
    assert result["sensor_id"] == "sensor_1"
    assert isinstance(result["timestamp"], datetime)


def test_invalid_json():
    processor = DataProcessor()
    msg = b"{invalid json}"

    with pytest.raises(ValueError, match="Invalid JSON data"):
        processor.process(msg)


def test_missing_field():
    processor = DataProcessor()
    msg = json.dumps({
        "sensor_id": "sensor_1",
        "timestamp": datetime.now().isoformat(),
        # missing temperature
        "humidity": 50.0,
        "pressure": 1000.0
    }).encode("utf-8")

    with pytest.raises(ValueError, match="Missing required field: temperature"):
        processor.process(msg)
