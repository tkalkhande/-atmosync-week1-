with sensor_data as (
    select * from {{ ref('stg_sensor_readings') }}
),

commodity_data as (
    select * from {{ ref('stg_commodity_prices') }}
),

joined as (
    select
        sensor_data.container_id,
        sensor_data.origin,
        sensor_data.destination,
        sensor_data.temperature_celsius,
        sensor_data.humidity_percent,
        sensor_data.vibration_level,
        sensor_data.event_timestamp,
        commodity_data.commodity,
        commodity_data.price_per_unit,
        commodity_data.event_date
    from sensor_data
    left join commodity_data
        on sensor_data.destination = commodity_data.market
        and date(sensor_data.event_timestamp) = commodity_data.event_date
)

select * from joined