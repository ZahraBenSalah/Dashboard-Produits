from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(topic: str, record: dict):
    """
    Envoie un dictionnaire JSON à un topic Kafka
    """
    producer.send(topic, record)
    producer.flush()
    print(f"✅ Message envoyé sur le topic '{topic}' : {record}")