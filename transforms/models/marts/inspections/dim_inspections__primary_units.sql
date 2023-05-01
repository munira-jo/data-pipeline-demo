with data as (
    select distinct unit_type_desc,
    unit_make,
    unit_license,
    unit_license_state,
    vin,
    unit_decal_number
    from {{ref('stg__inspections')}}
)

select {{ dbt_utils.generate_surrogate_key(['vin',
'unit_license',
'unit_type_desc',
'unit_make',
'unit_license_state',
'unit_decal_number'
]) }} as  primary_unit_id,
unit_type_desc,
unit_make,
unit_license,
unit_license_state,
vin,
unit_decal_number
from data
