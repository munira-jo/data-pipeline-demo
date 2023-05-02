with data as (
select distinct
vehicle_license_number,
vehicle_license_state,
vehicle_id_number,
report_seq_no
from {{ ref('stg__crashes') }}
)

select {{ dbt_utils.generate_surrogate_key(['vehicle_license_number',
'vehicle_id_number',
'report_seq_no',
'vehicle_license_state']) }} as vehicle_id,
vehicle_license_number,
vehicle_license_state,
vehicle_id_number,
report_seq_no
from data
