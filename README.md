# AtmoSync - Week 1: Ingestion Architecture + BI Foundations

Micro-Climate Arbitrage Analytics project for Infotact Solutions.

## What this does

A Python simulator generates fake shipping-container sensor data (temperature,
humidity, vibration) and streams it through Kafka into Snowflake, where it's
visualized in Apache Superset.

Simulator → Kafka → Consumer → Snowflake → Superset

## Setup

1. Start Kafka: `docker compose up -d`
2. Install Python deps: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in your real Snowflake credentials
4. Run the simulator: `python simulator.py`
5. In a second terminal, run the consumer: `python consumer.py`
6. Check data landing in Snowflake:
   `SELECT COUNT(*) FROM microclimate_db.raw_data.sensor_readings;`

## Superset (BI dashboard)

Superset runs as a separate Docker container, connected to the same Snowflake
warehouse. It has two user roles configured:
- **Admin** - full access
- **Gamma (viewer)** - read-only dashboard access, for stakeholders

A baseline chart ("Average Container Temperature") confirms the Superset →
Snowflake connection is working, pulling live data from the pipeline above.

## Tech stack

Apache Kafka, Snowflake, Apache Superset, Python (kafka-python,
snowflake-connector-python)