with data as (
    select distinct     trafficway_desc,
    weather_condition_desc,
    light_condition_desc,
    road_surface_condition_desc
    from {{ref('stg__crashes')}}
)

select {{ dbt_utils.generate_surrogate_key(['trafficway_desc',
'weather_condition_desc',
'light_condition_desc',
'road_surface_condition_desc']) }} as  condition_id,
Trafficway_Desc,
Weather_condition_desc,
Light_condition_desc,
Road_surface_condition_desc
from data
