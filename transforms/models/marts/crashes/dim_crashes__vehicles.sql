with data as (
    select *
    from {{ref('stg__crashes')}}
)

select {{ dbt_utils.generate_surrogate_key(['vehicle_id_number','vehicle_license_number','report_seq_no']) }} as vehicle_id,
vehicle_license_number,
vehicle_license_state,
vehicle_id_number,
report_seq_no
from data
