# toolkit

## Common Code Snippets

### Logging Setup

To set up consistent logging across scripts, you can use the following template:

```python
from logging_config import setup_logging

logger = setup_logging('my_script.log')
logger.info("This is an info log message")

### Placeholder Values

The ETL workflow and database connection details in `etl_with_prefect.py` use placeholder values. Before running the full workflow, replace these placeholders with your actual PostgreSQL credentials:

- **Username**: Replace `your_username` with your PostgreSQL username.
- **Password**: Replace `your_password` with your PostgreSQL password.
- **Database Name**: Replace `your_database` with the name of your database.
- **Host and Port**: Typically `localhost` and `5432` for local development, but adjust if connecting to a remote server.

Example Connection String:
```python
connection_string = "postgresql://your_username:your_password@localhost:5432/your_database"

### Kafka Monitoring

The toolkit includes a Kafka lag monitoring script (`monitor_kafka_lag.py`) in the `scripts/monitoring` folder. This script calculates and displays the lag for each Kafka partition and consumer group.

- **Configuration**: Update the following placeholders in the script:
  - `KAFKA_BROKER_URL`: The URL of your Kafka broker (e.g., `localhost:9092`).
  - `TOPIC_NAME`: The Kafka topic to monitor.
  - `CONSUMER_GROUP`: The consumer group to monitor.

- **Run the Script**:
  ```bash
  python3 scripts/monitoring/monitor_kafka_lag.py

### Pipeline Deployment

The `docker-compose.yml` file in the `infrastructure` directory sets up a local environment for the ETL pipeline, including PostgreSQL, Kafka, and Prefect services.

#### Steps to Deploy Locally

1. **Start Services**:
   ```bash
   cd infrastructure
   docker-compose up -d

