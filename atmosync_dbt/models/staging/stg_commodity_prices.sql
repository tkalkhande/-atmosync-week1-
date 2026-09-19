with source as (
    select * from {{ source('raw_data', 'commodity_prices') }}
),

staged as (
    select
        commodity,
        market,
        price_per_unit,
        event_date
    from source
    where commodity is not null
      and market is not null
)

select * from staged