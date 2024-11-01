from kafka import KafkaConsumer
import json
import psycopg2

# Kafka consumer configuration
consumer = KafkaConsumer(
    'your_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PostgreSQL connection setup
conn = psycopg2.connect(
    host="localhost",
    database="your_database",
    user="your_user",
    password="your_password"
)
cursor = conn.cursor()

# Process and insert messages
for message in consumer:
    data = message.value
    cursor.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (data['key1'], data['key2']))
    conn.commit()

cursor.close()
conn.close()

