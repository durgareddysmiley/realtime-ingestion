import sqlite3


class DataStorage:
    def __init__(self, db_path="data/processed_data.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.create_table()

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            sensor_id TEXT,
            timestamp TEXT,
            temperature REAL,
            humidity REAL,
            pressure REAL
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def insert(self, data):
        insert_query = """
        INSERT INTO sensor_readings
        (sensor_id, timestamp, temperature, humidity, pressure)
        VALUES (?, ?, ?, ?, ?)
        """
        self.conn.execute(
            insert_query,
            (
                data["sensor_id"],
                data["timestamp"].isoformat(),
                data["temperature"],
                data["humidity"],
                data["pressure"]
            )
        )
        self.conn.commit()
