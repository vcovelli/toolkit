from kafka import KafkaProducer
import json

# Configure the Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_message(topic, message):
    producer.send(topic, message)
    producer.flush()

# Example usage
if __name__ == "__main__":
    produce_message('your_topic', {'key': 'value'})

