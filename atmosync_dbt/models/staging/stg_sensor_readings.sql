{{ config(materialized='table', cluster_by=['event_timestamp']) }}
with source as (
    select * from {{ source('raw_data', 'sensor_readings') }}
),

staged as (
    select
        sensor_id as container_id,
        temperature_celsius,
        humidity_percent,
        vibration_level,
        origin,
        destination,
        event_timestamp,
        loaded_at
    from source
    where sensor_id is not null
      and event_timestamp is not null
)

select * from staged