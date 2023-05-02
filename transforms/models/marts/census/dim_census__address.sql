with data as (
    select distinct phy_street,
phy_city,
phy_state,
phy_zip,
phy_country,
mailing_street,
mailing_city,
mailing_state,
mailing_zip,
mailing_country
    from {{ref('stg__census')}}
)

select {{ dbt_utils.generate_surrogate_key(['phy_street',
'phy_city',
'phy_state',
'phy_zip',
'phy_country',
'mailing_street',
'mailing_city',
'mailing_state',
'mailing_zip']) }} as  address_id,
phy_street,
phy_city,
phy_state,
phy_zip,
phy_country,
mailing_street,
mailing_city,
mailing_state,
mailing_zip,
mailing_country
from data
