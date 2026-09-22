-- fct_spoilage_arbitrage.sql
-- Core Week 3 mart: combines estimated hours-to-spoilage with distance to the
-- nearest alternate market to flag "At-Risk" containers and recommend reroutes.
--
-- ASSUMPTION (flag in review): average ocean-freight transit speed of
-- 500 km/day (~20.8 km/hr) is used to convert distance -> transit hours.
-- This is a rough industry-typical figure for container shipping, not
-- route-specific — swap in real routing data if/when available.

with spoilage as (

    select * from {{ ref('int_spoilage_rate') }}

),

nearest_market as (

    select
        current_destination,
        candidate_market,
        distance_km
    from {{ ref('int_distance_to_market') }}
    where proximity_rank = 1

),

joined as (

    select
        s.container_id,
        s.origin,
        s.destination,
        s.commodity,
        s.temperature_celsius,
        s.event_timestamp,
        s.estimated_hours_to_spoilage,
        nm.candidate_market as nearest_alternate_market,
        nm.distance_km,
        nm.distance_km / (500.0 / 24.0) as estimated_transit_hours_to_alternate
    from spoilage s
    left join nearest_market nm
        on s.destination = nm.current_destination

)
select
    container_id,
    origin,
    destination,
    commodity,
    temperature_celsius,
    event_timestamp,
    round(estimated_hours_to_spoilage, 1)          as estimated_hours_to_spoilage,
    nearest_alternate_market,
    round(distance_km, 1)                          as distance_to_alternate_km,
    round(estimated_transit_hours_to_alternate, 1) as estimated_transit_hours_to_alternate,
    case
        when ntile(4) over (partition by commodity order by estimated_hours_to_spoilage) = 1
            then true
        else false
    end as is_at_risk,
    case
        when ntile(4) over (partition by commodity order by estimated_hours_to_spoilage) = 1
            then nearest_alternate_market
        else null
    end as recommended_reroute_destination
from joined