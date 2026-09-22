with current_dest as (
    select distinct
        f.destination,
        m.latitude  as origin_lat,
        m.longitude as origin_lon
    from {{ ref('fct_sensor_commodity_arbitrage') }} f
    left join {{ ref('market_locations') }} m
        on f.destination = m.market
),

candidates as (
    select
        latitude  as candidate_lat,
        longitude as candidate_lon,
        market    as candidate_market
    from {{ ref('market_locations') }}
),

distances as (
    select
        cd.destination as current_destination,
        c.candidate_market,
        6371 * acos(
            least(1.0,
                cos(radians(cd.origin_lat)) * cos(radians(c.candidate_lat))
                * cos(radians(c.candidate_lon) - radians(cd.origin_lon))
                + sin(radians(cd.origin_lat)) * sin(radians(c.candidate_lat))
            )
        ) as distance_km
    from current_dest cd
    cross join candidates c
    where cd.destination != c.candidate_market
)

select
    current_destination,
    candidate_market,
    distance_km,
    row_number() over (
        partition by current_destination
        order by distance_km asc
    ) as proximity_rank
from distances