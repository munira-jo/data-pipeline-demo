with data as (
    select distinct email_address,
    telephone,
    fax
    from {{ref('stg__census')}}
)

select {{ dbt_utils.generate_surrogate_key(['email_address',
'telephone',
'fax']) }} as  contact_id,
email_address,
telephone,
fax
from data
