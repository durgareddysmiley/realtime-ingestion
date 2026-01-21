# Real-time Sensor Data Ingestion Microservice

A robust, containerized Python-based microservice that consumes real-time sensor data from Apache Kafka, performs schema validation and transformation, and persists the results to a SQLite database.

## 🚀 Key Features

- **Event-Driven Architecture**: Leverages Apache Kafka for high-throughput data streaming.
- **Robust Processing**: Includes a dedicated `DataProcessor` module for rigorous schema and type validation.
- **Dead Letter Queue (DLQ)**: Fault tolerance mechanism that redirects invalid messages to a separate Kafka topic for debugging.
- **Containerized**: Fully orchestrated using Docker Compose (Zookeeper, Kafka, Producer, Consumer).
- **Graceful Shutdown**: Properly handles termination signals to ensure data integrity and offset commitment.
- **Unit Tested**: Comprehensive test suite for processing and storage logic.

## 🛠 Tech Stack

- **Lanuage**: Python 3.9+
- **Message Broker**: Apache Kafka
- **Database**: SQLite
- **Infrastructure**: Docker, Docker Compose
- **Testing**: PyTest

## 📦 Project Structure

```text
realtime-ingestion/
├── src/
│   ├── consumer.py        # Main entry point, coordinates data flow
│   ├── processor.py       # Validation and transformation logic
│   ├── storage.py         # SQLite persistence logic
│   └── producer.py        # Synthetic data generator
├── data/                  # Persistent storage directory
├── tests/                 # Unit test suite
├── Dockerfile             # Container configuration
├── docker-compose.yml     # Service orchestration
├── requirements.txt       # Python dependencies
├── ARCHITECTURE.md        # Detailed technical overview
└── README.md              # Project documentation
```

## 🚦 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Installation & Running

1. **Clone the repository**:
   ```bash
   git clone <repo_url>
   cd realtime-ingestion
   ```

2. **Start the pipeline**:
   ```bash
   docker-compose up --build
   ```
   This will launch Zookeeper, Kafka, the synthetic data producer, and the ingestion service.

### Verifying the Data

You can verify that data is being processed and stored by querying the SQLite database inside the `ingestion` container:

```bash
docker-compose exec ingestion sqlite3 data/processed_data.db "SELECT * FROM sensor_readings LIMIT 10;"
```

### Running Tests

To run the unit tests locally:

```bash
# Setup environment
pip install -r requirements.txt
export PYTHONPATH=$PYTHONPATH:./src

# Run tests
pytest tests/
```

## 📐 Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed breakdown of the system components and data flow diagrams.

## 🛡 License

Distributed under the MIT License. See `LICENSE` for more information.
