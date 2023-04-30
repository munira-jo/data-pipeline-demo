with source as (
    select *
    from {{source('staging','crash')}}
)

select 
-- ids
report_number,
report_seq_no,
dot_number,
vehicle_id_number,
vehicle_license_number, 

--strings
report_state,
tow_away,
hazmat_released,
trafficway_desc,
access_control_desc,
road_surface_condition_desc,
weather_condition_desc,      
light_condition_desc,  
not_preventable,  
citation_issued_desc,    
vehicle_license_state,


--numbers
fatalities,
injuries,
seq_num,
time_weight,
severity_weight,

--dates
report_date

from source