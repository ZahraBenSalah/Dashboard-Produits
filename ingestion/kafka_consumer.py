from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'api-topic',  # pour API
    'csv-topic',  # pour CSV
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consumer démarré, en attente des messages...")

for message in consumer:
    print(message.value)