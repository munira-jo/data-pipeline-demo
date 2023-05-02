with data as (
    select distinct  hazmat_released,
    tow_away,
    time_weight,
    severity_weight
    from {{ref('stg__crashes')}}
)

select {{ dbt_utils.generate_surrogate_key(['hazmat_released',
'tow_away',
'time_weight',
'severity_weight']) }} as  result_id,
hazmat_released,
    tow_away,
    time_weight,
    severity_weight
from data
