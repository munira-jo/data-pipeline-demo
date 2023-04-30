with data as (
    select distinct  report_number,
    report_state,
    seq_num,
    dot_number,
    not_preventable,
    citation_issued_desc,
    report_date
    from {{ref('stg__crashes')}}
)

select {{ dbt_utils.generate_surrogate_key(['seq_num',
'report_state',
'dot_number',
'report_number',
'report_date']) }} as  report_id,
report_number,
report_state,
dot_number,
seq_num,
not_preventable,
citation_issued_desc,
report_date
from data
