with data as (SELECT
distinct unique_id,
dot_number,
viol_code,
basic_desc,
oos_indicator,
viol_unit,
group_desc,
section_desc,
insp_date
from {{ref('stg__violations')}})

select 
-- ids
 {{ dbt_utils.generate_surrogate_key(['unique_id',
'dot_number',
'viol_code']) }} as  violation_id,
unique_id,
dot_number,
viol_code,
basic_desc,
oos_indicator,
viol_unit,
group_desc,
section_desc,
insp_date

from data