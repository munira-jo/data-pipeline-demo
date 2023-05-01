with data as (
    select distinct unit_type_desc2,
    unit_make2,
    unit_license2,
    unit_license_state2,
    vin2,
    unit_decal_number2
    from {{ref('stg__inspections')}}
)

select {{ dbt_utils.generate_surrogate_key(['vin2',
'unit_license2',
'unit_type_desc2',
'unit_decal_number2',
'unit_license_state2',
'unit_make2'
]) }} as  secondary_unit_id,
unit_type_desc2,
unit_make2,
unit_license2,
unit_license_state2,
vin2,
unit_decal_number2
from data
