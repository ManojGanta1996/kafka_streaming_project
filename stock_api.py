from fastapi import FastAPI
from fastapi.responses import JSONResponse
from faker import Faker
from typing import List
import random
import time
import threading

app = FastAPI()
fake = Faker()

latest_data = {}  # holds the most recent record

def generate_trade():
    return {
        "exchange": random.choice(["NYSE", "NASDAQ", "LSE", "BSE", "HKEX"]),
        "timestamp": fake.iso8601(),
        "symbol": fake.lexify(text='???'),
        "company": fake.company(),
        "price": round(random.uniform(10, 1000), 2),
        "volume": random.randint(100, 10000),
        "broker": {
            "name": fake.name(),
            "id": fake.uuid4(),
            "location": {
                "city": fake.city(),
                "country": fake.country()
            }
        },
        "order_book": [
            {
                "bid_price": round(random.uniform(10, 999), 2),
                "bid_volume": random.randint(50, 5000),
                "ask_price": round(random.uniform(10, 999), 2),
                "ask_volume": random.randint(50, 5000)
            } for _ in range(5)
        ]
    }

def stream_data():
    global latest_data
    while True:
        latest_data = generate_trade()
        time.sleep(1)  # simulate 1-second interval

@app.get("/live")
def get_live_data():
    """Get the latest streamed stock data"""
    return JSONResponse(content=latest_data)

@app.get("/health")
def health_check():
    return {"status": "running"}

# Start background thread for data generation
threading.Thread(target=stream_data, daemon=True).start()
