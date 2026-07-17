import json
import tempfile
import os
import random
import time
from datetime import datetime, UTC

#Temp folder handling
BASE_TEMP_DIR = r"C:\tmp"
LANDING_DIR = os.path.join(BASE_TEMP_DIR, "analytics", "landing")
DB_PATH = os.path.join(BASE_TEMP_DIR, "analytics", "warehouse.db")

#Local storage zones
os.makedirs(LANDING_DIR, exist_ok=True)



EVENTS = ["page_view", "product_view", "add_to_cart", "purchase"]
DEVICES = ["dektop", "mobile", "tablet"]
PRODUCTS = [
    {"id": "P101", "name": "Mechanical Keyboard", "price": 120.00},
    {"id": "P102", "name": "Ergonomic Mouse", "price": 85.50},
    {"id": "P103", "name": "4K Monitor", "price": 349.99}
]

def generate_clickstream_event():
    """Generates simulated dataLayer push"""
    user_id = f"USR_{random.randint(1000, 1050)}"
    session_id = f"SESS_{random.randint(1000, 99999)}"
    event_type = random.choice(EVENTS)

    payload = {
        "event_id": f"evt_{random.randint(1000000, 9999999)}",
        "timestamp": datetime.now(UTC).isoformat(),
        "event_name": event_type,
        "user_context":{
            "user_id": user_id,
            "session_id": session_id,
            "device": random.choice(DEVICES)
        },
        "event_properties": {}
    }

    #Dynamically insert the values for event_properties if it's purchase related
    if event_type in ["product_view", "add_to_cart", "purchase"]:
        product = random.choice(PRODUCTS)
        payload["event_properties"] = {
            "product_id": product["id"],
            "product_name": product["name"],
            "product_price": product["price"],
            "currency": "USD"
        }
    return payload

def simulate_stream(batch_size=20):
    """Writes batches of JSON payloads into landing folder"""
    timestamp_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"clickstream_{timestamp_str}.json"
    filepath = os.path.join(LANDING_DIR, filename)

    events_batch = []
    for _ in range(batch_size):
        events_batch.append(generate_clickstream_event())

        #5% chance of duplicate event
        if random.random() < 0.05 and events_batch:
            events_batch.__contains__(events_batch[-1])
    
    with open(filepath, "w") as f:
        for event in events_batch:
            f.write(json.dumps(event) + "\n")
    
    print(f"SUCCESS, Dropped {len(events_batch)} raw events into landing: {filename}")
    print(f"Base Directory: {BASE_TEMP_DIR}")

if __name__ == "__main__":
    print("Starting streaming event simulation, press Ctrl + C to stop.")
    try:
        while True:
            simulate_stream(batch_size=random.randint(10, 30))
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nGenerator stopped")