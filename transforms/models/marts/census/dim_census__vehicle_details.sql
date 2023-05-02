with data as (
    select distinct  dot_number,
    legal_name,
    dba_name,
    carrier_operation,
    hm_flag,
    pc_flag,
    add_date,
    mcs150_date,
    vmt_source_id,
    mcs150_mileage_year,
    recent_mileage_year
    from {{ref('stg__census')}}
)

select {{ dbt_utils.generate_surrogate_key(['dot_number',
'carrier_operation',
'legal_name']) }} as  vehicle_detail_id,
legal_name,
dba_name,
carrier_operation,
hm_flag,
pc_flag,
add_date,
mcs150_date,
vmt_source_id,
mcs150_mileage_year,
recent_mileage_year
from data
