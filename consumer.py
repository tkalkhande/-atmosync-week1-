import json
from kafka import KafkaConsumer
import snowflake.connector

# --- Fill in your own Snowflake details here ---
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "sensor_readings"
TOPIC = "climate-sensor-data"

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def insert_reading(cursor, reading):
    # Week 2 update: now also persisting origin and destination
    # alongside humidity_percent/vibration_level.
    cursor.execute(
        f"""
        INSERT INTO {SNOWFLAKE_TABLE} (sensor_id, temperature_celsius, humidity_percent, vibration_level, origin, destination, event_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            reading["container_id"],
            reading["temperature_celsius"],
            reading["humidity_percent"],
            reading["vibration_level"],
            reading["origin"],
            reading["destination"],
            reading["timestamp"]
        )
    )

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9094",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="snowflake-loader-v3"
    )

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print(f"Connected to Snowflake. Listening on topic '{TOPIC}'... Press Ctrl+C to stop.")

    try:
        for message in consumer:
            reading = message.value
            insert_reading(cursor, reading)
            conn.commit()
            print(f"Inserted into Snowflake: {reading}")
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()