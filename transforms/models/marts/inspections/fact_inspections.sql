with data as 
(SELECT *
from {{ref('stg__inspections')}}
),
violations as (
    select *
    from {{ ref('stg__violations') }}
)

select {{ dbt_utils.generate_surrogate_key(['data.unique_id',
'report_number',
'data.dot_number',
'data.insp_date',
'viol_code'
]) }} as  inspection_id,
{{ dbt_utils.generate_surrogate_key(['insp_level_id',
'data.insp_date',
'hazmat_placard_req'
]) }} as  inspection_detail_id,
{{ dbt_utils.generate_surrogate_key(['data.unique_id',
'report_state',
'data.dot_number',
'report_number',
'county_code_state']) }} as  report_id,
{{ dbt_utils.generate_surrogate_key(['vin',
'unit_license',
'unit_type_desc',
'unit_make',
'unit_license_state',
'unit_decal_number'
]) }} as  primary_unit_id,
{{ dbt_utils.generate_surrogate_key(['vin2',
'unit_license2',
'unit_type_desc2',
'unit_decal_number2',
'unit_license_state2',
'unit_make2'
]) }} as  secondary_unit_id,
 {{ dbt_utils.generate_surrogate_key(['violations.unique_id',
'violations.dot_number',
'viol_code']) }} as  violation_id,
data.time_weight as inspection_time_weight,
unsafe_viol,
fatigued_viol,
dr_fitness_viol,
subt_alcohol_viol,
vh_maint_viol,
hm_viol,
driver_oos_total,
vehicle_oos_total,
total_hazmat_sent,
oos_total,
hazmat_oos_total,
violations.oos_weight as violation_oos_weight,
severity_weight as violation_severity_weight,
violations.time_weight as violation_time_weight,
viol_value,
tot_severity_wght as violation_total_severity_weight
from data
left join violations on violations.unique_id = data.unique_id
and violations.dot_number = data.dot_number
and violations.insp_date = data.insp_date