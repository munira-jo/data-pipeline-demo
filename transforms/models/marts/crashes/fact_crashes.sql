with conditions as (
    select *
    from {{ref('dim_crashes__conditions')}}
),
reports as (
      select *
    from {{ref('dim_crashes__reports')}}  
),
results  as (
      select *
    from {{ref('dim_crashes__results')}}  
),
vehicles  as (
      select *
    from {{ref('dim_crashes__vehicles')}}  
),
raw_data as (
    select *
    from {{ref('stg__crashes')}}
)

select {{ dbt_utils.generate_surrogate_key(['seq_num',
'report_state',
'dot_number',
'report_number',
'report_date']) }} as  report_id,
{{ dbt_utils.generate_surrogate_key(['vehicle_license_number','vehicle_id_number','report_seq_no','vehicle_license_state']) }} as vehicle_id,
{{ dbt_utils.generate_surrogate_key(['trafficway_desc',
'weather_condition_desc',
'light_condition_desc',
'road_surface_condition_desc']) }} as  conditions_id,
{{ dbt_utils.generate_surrogate_key(['hazmat_released',
'tow_away',
'time_weight',
'severity_weight']) }} as  result_id,
fatalities,
injuries
from raw_data
 