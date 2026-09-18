import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Connect to Kafka running in Docker.
# We use localhost:9094 because that's the EXTERNAL listener
# we set up in docker-compose.yml for connections from outside Docker.
producer = KafkaProducer(
    bootstrap_servers="localhost:9094",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "climate-sensor-data"

# Simulating a small fleet of shipping containers, matching the
# "AtmoSync" use case (e.g. Container A carrying avocados).
CONTAINER_IDS = ["CONTAINER-A", "CONTAINER-B", "CONTAINER-C"]

CONTAINER_ROUTES = {
    "CONTAINER-A": {"origin": "Mumbai", "destination": "Rotterdam"},
    "CONTAINER-B": {"origin": "Santos", "destination": "Hamburg"},
    "CONTAINER-C": {"origin": "Ho Chi Minh City", "destination": "Los Angeles"},
}

def generate_reading(container_id):
    route = CONTAINER_ROUTES[container_id]
    return {
        "container_id": container_id,
        "origin": route["origin"],
        "destination": route["destination"],
        "temperature_celsius": round(random.uniform(2.0, 12.0), 2),
        "humidity_percent": round(random.uniform(70.0, 95.0), 2),
        "vibration_level": round(random.uniform(0.0, 5.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def main():
    print(f"Starting simulator. Publishing to topic '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            for container_id in CONTAINER_IDS:
                reading = generate_reading(container_id)
                producer.send(TOPIC, value=reading)
                print(f"Sent: {reading}")
            producer.flush()
            time.sleep(2)  # send a new round of readings every 2 seconds
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()