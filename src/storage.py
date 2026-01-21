import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

class DataStorage:
    """Handles persistence of sensor data to SQLite."""

    def __init__(self, db_path="data/processed_data.db"):
        self.db_path = db_path
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.create_table()
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise

    def create_table(self):
        """Creates the sensor_readings table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            temperature REAL,
            humidity REAL,
            pressure REAL
        )
        """
        try:
            self.conn.execute(query)
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error creating table: {e}")
            raise

    def insert(self, data):
        """Inserts a single sensor reading into the database."""
        insert_query = """
        INSERT INTO sensor_readings
        (sensor_id, timestamp, temperature, humidity, pressure)
        VALUES (?, ?, ?, ?, ?)
        """
        try:
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
        except sqlite3.Error as e:
            logger.error(f"Error inserting data: {e}")
            raise
            
    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
