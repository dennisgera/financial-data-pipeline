with 

daily_stock_prices as (
    select *
    from {{ ref('stg_alphavantage__daily_stock_prices') }}
),

date_spine as (
    -- Generate a complete date series without gaps
    select generate_series(
        (select min(date) from daily_stock_prices),
        (select max(date) from daily_stock_prices),
        '1 day'
    )::date as trading_date
),

symbols as (
    -- Get distinct symbols for cross join
    select distinct symbol 
    from daily_stock_prices
),

date_symbol_spine as (
    -- Create complete date-symbol combinations
    select 
        trading_date,
        symbol
    from date_spine
    cross join symbols
),

stock_metrics as (
    select
        date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        
        -- Price change metrics
        close - open as daily_price_change,
        ((close - open) / open) * 100 as daily_price_change_pct,
        
        -- Volatility metrics
        (high - low) as daily_price_range,
        ((high - low) / open) * 100 as daily_volatility_pct,
        
        -- Volume metrics
        volume as daily_volume,
        
        -- Trading metrics
        case 
            when close > open then 'BULLISH'
            when close < open then 'BEARISH'
            else 'NEUTRAL'
        end as daily_trend,
        
        -- Moving averages
        avg(close) over(
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) as moving_avg_5d,
        
        avg(close) over(
            partition by symbol 
            order by date 
            rows between 19 preceding and current row
        ) as moving_avg_20d,
        
        -- Relative volume
        volume / avg(volume) over(
            partition by symbol 
            order by date 
            rows between 4 preceding and current row
        ) as relative_volume_5d
        
    from daily_stock_prices
),

final as (
    select
        ds.trading_date,
        ds.symbol,
        coalesce(sm.open, lag(sm.close) over(partition by ds.symbol order by ds.trading_date)) as open,
        coalesce(sm.high, lag(sm.close) over(partition by ds.symbol order by ds.trading_date)) as high,
        coalesce(sm.low, lag(sm.close) over(partition by ds.symbol order by ds.trading_date)) as low,
        coalesce(sm.close, lag(sm.close) over(partition by ds.symbol order by ds.trading_date)) as close,
        coalesce(sm.volume, 0) as volume,
        coalesce(sm.daily_price_change, 0) as daily_price_change,
        coalesce(sm.daily_price_change_pct, 0) as daily_price_change_pct,
        coalesce(sm.daily_price_range, 0) as daily_price_range,
        coalesce(sm.daily_volatility_pct, 0) as daily_volatility_pct,
        coalesce(sm.daily_volume, 0) as daily_volume,
        coalesce(sm.daily_trend, 'NO_TRADING') as daily_trend,
        coalesce(sm.moving_avg_5d, 0) as moving_avg_5d,
        coalesce(sm.moving_avg_20d, 0) as moving_avg_20d,
        coalesce(sm.relative_volume_5d, 0) as relative_volume_5d,
        case 
            when sm.date is null then true 
            else false 
        end as is_trading_holiday,
        current_timestamp as model_last_refreshed
    from date_symbol_spine ds
    left join stock_metrics sm 
        on ds.trading_date = sm.date 
        and ds.symbol = sm.symbol
)

select * from final