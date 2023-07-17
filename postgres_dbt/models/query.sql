{{ config(alias='query_'+var("today_date")) }}

SELECT city,weather_date,temperature_MAX_C,temperature_MIN_C,sunrise,sunset,precipitation,description from 
weather_data, {{ref('codes')}} where codes.code = weather_data.weather_code and 
weather_date > '{{var("today_date")}}'::date - 7
order by city,weather_date