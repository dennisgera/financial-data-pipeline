with source as (

    select * from {{ source('alphavantage', 'daily_stock_prices') }}

),

renamed as (

    select
        date::DATE as date,
        open::NUMERIC as open,
        high::NUMERIC as high,
        low::NUMERIC as low,
        close::NUMERIC as close,
        volume::NUMERIC as volume,
        symbol::TEXT as symbol,
        last_refreshed::DATE as last_refreshed,
        loaded_at::TIMESTAMPTZ as loaded_at

    from source

)

select * from renamed