with data as (
    select distinct unique_id,
    report_number,
    report_state,
    dot_number,
    county_code_state
    from {{ref('stg__inspections')}}
)

select {{ dbt_utils.generate_surrogate_key(['unique_id',
'report_state',
'dot_number',
'report_number',
'county_code_state']) }} as  report_id,
report_number,
report_state,
dot_number,
county_code_state
from data
