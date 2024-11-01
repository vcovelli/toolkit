from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Kafka configuration placeholders
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "your_topic_name"
CONSUMER_GROUP = "your_consumer_group"

def check_kafka_lag():
    consumer = KafkaConsumer(
        group_id=CONSUMER_GROUP,
        bootstrap_servers=[KAFKA_BROKER_URL],
        enable_auto_commit=False
    )

    partitions = consumer.partitions_for_topic(TOPIC_NAME)
    if partitions is None:
        print(f"No partitions found for topic {TOPIC_NAME}")
        return

    lags = {}
    for partition in partitions:
        tp = consumer.assignment().pop()  # assuming one partition at a time
        committed_offset = consumer.committed(tp)
        end_offset = consumer.end_offsets([tp])[tp]
        lag = end_offset - committed_offset if committed_offset is not None else None
        lags[partition] = lag
        print(f"Partition {partition}: Lag = {lag}")

    consumer.close()
    return lags

if __name__ == "__main__":
    while True:
        print("Checking Kafka lag...")
        check_kafka_lag()
        time.sleep(10)  # Check every 10 seconds

