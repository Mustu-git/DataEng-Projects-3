/*
  Joins daily weather conditions with taxi trip volume from Project 1.
  Shows how temperature, precipitation and wind affect daily demand.
*/

with weather_daily as (

    select
        weather_date,
        round(avg(temperature_f)::numeric, 1)       as avg_temp_f,
        round(sum(precipitation_mm)::numeric, 2)    as total_precip_mm,
        round(avg(windspeed_kmh)::numeric, 1)       as avg_wind_kmh,
        bool_or(is_raining)                         as had_rain,
        bool_or(is_windy)                           as had_wind,
        count(*) filter (where is_raining)          as rainy_hours,
        count(*) filter (where is_windy)            as windy_hours
    from {{ ref('stg_weather_hourly') }}
    group by 1

),

taxi_daily as (

    select
        trip_date,
        trip_count,
        total_revenue,
        avg_fare,
        avg_trip_distance
    from {{ source('staging', 'gold_daily_trips') }}

),

joined as (

    select
        t.trip_date,
        t.trip_count,
        t.total_revenue,
        t.avg_fare,
        t.avg_trip_distance,
        w.avg_temp_f,
        w.total_precip_mm,
        w.avg_wind_kmh,
        w.had_rain,
        w.had_wind,
        w.rainy_hours,
        w.windy_hours,

        -- Weather condition label for easy filtering
        case
            when w.total_precip_mm > 5  then 'heavy_rain'
            when w.total_precip_mm > 0  then 'light_rain'
            when w.had_wind             then 'windy'
            else 'clear'
        end                                         as weather_condition

    from taxi_daily t
    left join weather_daily w on w.weather_date = t.trip_date

)

select * from joined
order by trip_date
