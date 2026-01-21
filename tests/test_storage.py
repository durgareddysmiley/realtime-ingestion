import os
from src.storage import DataStorage
from datetime import datetime


def test_insert_and_store_data():
    test_db = "data/test_processed_data.db"

    # Remove test DB if it already exists
    if os.path.exists(test_db):
        os.remove(test_db)

    storage = DataStorage(db_path=test_db)

    sample_data = {
        "sensor_id": "sensor_test",
        "timestamp": datetime.now(),
        "temperature": 26.5,
        "humidity": 55.0,
        "pressure": 1012.0
    }

    storage.insert(sample_data)

    # Verify data exists
    cursor = storage.conn.cursor()
    cursor.execute("SELECT * FROM sensor_readings")
    rows = cursor.fetchall()

    assert len(rows) == 1
    # Column 0 is 'id', Column 1 is 'sensor_id'
    assert rows[0][1] == "sensor_test"
