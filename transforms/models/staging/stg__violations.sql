with source as (
    select *
    from {{source('staging','violation')}}
)

select 
-- ids
unique_id,
dot_number,

--strings
viol_code,
basic_desc,
oos_indicator,
viol_unit,
group_desc,
section_desc,

--numbers
oos_weight,
severity_weight,
time_weight,
viol_value,
tot_severity_wght,

--dates
insp_date

from source