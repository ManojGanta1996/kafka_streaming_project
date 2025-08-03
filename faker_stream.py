from faker import Faker
from confluent_kafka import Producer
import json
import time
import random

fake = Faker()

# Kafka config
conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)

topic = 'stock-prices'

def generate_nested_json():
    return {
        "user": {
            "id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "country": fake.country()
            }
        },
        "transaction": {
            "id": fake.uuid4(),
            "amount": round(random.uniform(10, 1000), 2),
            "currency": "GBP",
            "timestamp": fake.iso8601(),
            "items": [
                {
                    "item_id": fake.uuid4(),
                    "description": fake.word(),
                    "price": round(random.uniform(5, 300), 2)
                } for _ in range(random.randint(1, 3))
            ]
        }
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# Continuous stream
while True:
    data = generate_nested_json()
    json_data = json.dumps(data)
    producer.produce(topic, value=json_data, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)  # send every 1 second
