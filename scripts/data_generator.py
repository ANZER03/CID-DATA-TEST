import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'raw_transactions'

def generate_data():
    print("Starting data generation... Sending to Kafka:9092")
    while True:
        try:
            data = {
                "id_transaction": str(fake.uuid4()),
                "client": fake.name(),
                "produit": random.choice(["Laptop", "Smartphone", "Ecran", "Clavier", "Souris"]),
                "prix": float(random.randint(50, 5000)), 
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }
            producer.send(topic_name, value=data)
            print(f"Sent: {data['produit']} | {data['prix']} DH")
            time.sleep(1)
        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            time.sleep(5)

if __name__ == "__main__":
    generate_data()