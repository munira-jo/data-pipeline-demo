with data as 
(SELECT *
from {{ref('stg__inspections')}}
)

select {{ dbt_utils.generate_surrogate_key(['unique_id',
'report_number',
'dot_number',
'insp_date'
]) }} as  inspection_id,
{{ dbt_utils.generate_surrogate_key(['insp_level_id',
'insp_date',
'hazmat_placard_req'
]) }} as  inspection_detail_id,
{{ dbt_utils.generate_surrogate_key(['unique_id',
'report_state',
'dot_number',
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
hazmat_oos_total
from data