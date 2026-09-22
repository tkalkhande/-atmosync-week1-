-- int_spoilage_rate.sql
-- Estimates hours-to-spoilage for each sensor reading x commodity pairing.
--
-- FORMULA (documented assumption — flag this in review):
-- Uses a simplified Q10-style temperature sensitivity rule:
--   hours_to_spoilage = base_shelf_life_hours * 0.5 ^ ((actual_temp - ideal_temp) / q10_interval_c)
-- i.e. shelf life halves every `q10_interval_c` degrees above the commodity's ideal
-- temperature, and roughly doubles for every interval below it (capped, see below).
-- This is a standard approximation used in food-science shelf-life modeling
-- (not a precise chemical kinetics model) — good enough for a mock arbitrage demo.
--
-- Source: fct_sensor_commodity_arbitrage already joins sensor readings to
-- commodity_prices by destination + date, so it's the natural base here.

with base as (

    select
        container_id,
        origin,
        destination,
        commodity,
        temperature_celsius,
        humidity_percent,
        event_timestamp
    from {{ ref('fct_sensor_commodity_arbitrage') }}

),

joined as (

    select
        b.*,
        r.ideal_temp_c,
        r.base_shelf_life_hours,
        r.q10_interval_c
    from base b
    left join {{ ref('commodity_spoilage_reference') }} r
        on b.commodity = r.commodity

)

select
    container_id,
    origin,
    destination,
    commodity,
    temperature_celsius,
    humidity_percent,
    event_timestamp,
    ideal_temp_c,
    base_shelf_life_hours,
    q10_interval_c,
    -- cap the exponent multiplier at 4x so a very cold reading doesn't imply
    -- implausibly long shelf life
    least(
        base_shelf_life_hours * power(0.5, (temperature_celsius - ideal_temp_c) / q10_interval_c),
        base_shelf_life_hours * 4
    ) as estimated_hours_to_spoilage
from joined
where ideal_temp_c is not null
