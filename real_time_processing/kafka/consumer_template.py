from kafka import KafkaConsumer
import json

# Configure the Kafka Consumer
consumer = KafkaConsumer(
    'your_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")

