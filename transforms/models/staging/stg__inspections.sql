with source as (
    select *
    from {{source('staging','inspection')}}
)

select 
-- ids
unique_id,
dot_number,
insp_level_id,

--strings
county_code_state,
hazmat_placard_req,
unit_type_desc,
unit_make,
unit_license,
unit_license_state,
vin,
unit_decal_number,
unit_type_desc2,
unit_make2,
unit_license2,
unit_license_state2,
vin2,
unit_decal_number2,

--inspection types
unsafe_insp,
fatigued_insp,
dr_fitness_insp,
subt_alcohol_insp,
vh_maint_insp,
hm_insp,

--violation numbers
unsafe_viol,
fatigued_viol,
dr_fitness_viol,
subt_alcohol_viol,
vh_maint_viol,
hm_viol,


--numbers
time_weight,
driver_oos_total,
vehicle_oos_total,
total_hazmat_sent,
oos_total,
hazmat_oos_total,


--dates
insp_date

from source