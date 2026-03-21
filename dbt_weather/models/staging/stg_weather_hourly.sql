/*
  Unpacks raw JSON from raw.weather_raw into one row per hour.
  Uses jsonb_array_elements_text with WITH ORDINALITY to zip
  the parallel time/temp/precip/wind arrays by position.
*/

with raw as (

    select response_json
    from {{ source('raw', 'weather_raw') }}

),

times as (
    select
        response_json,
        value::timestamp as hour_ts,
        ordinality as pos
    from raw,
    jsonb_array_elements_text(response_json -> 'hourly' -> 'time')
        with ordinality
),

temps as (
    select
        response_json,
        value::numeric as temperature_c,
        ordinality as pos
    from raw,
    jsonb_array_elements_text(response_json -> 'hourly' -> 'temperature_2m')
        with ordinality
),

precip as (
    select
        response_json,
        value::numeric as precipitation_mm,
        ordinality as pos
    from raw,
    jsonb_array_elements_text(response_json -> 'hourly' -> 'precipitation')
        with ordinality
),

wind as (
    select
        response_json,
        value::numeric as windspeed_kmh,
        ordinality as pos
    from raw,
    jsonb_array_elements_text(response_json -> 'hourly' -> 'windspeed_10m')
        with ordinality
),

joined as (

    select
        t.hour_ts,
        te.temperature_c,
        round((te.temperature_c * 9.0 / 5.0 + 32)::numeric, 1)   as temperature_f,
        p.precipitation_mm,
        w.windspeed_kmh,
        case when p.precipitation_mm > 0 then true else false end  as is_raining,
        case when w.windspeed_kmh > 30   then true else false end  as is_windy,
        date_trunc('day', t.hour_ts)::date                         as weather_date
    from times t
    join temps  te on te.response_json = t.response_json and te.pos = t.pos
    join precip p  on p.response_json  = t.response_json and p.pos  = t.pos
    join wind   w  on w.response_json  = t.response_json and w.pos  = t.pos

)

select * from joined
where hour_ts is not null
order by hour_ts
