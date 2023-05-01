with data AS
(select *
from {{ ref('stg__census') }})

select {{ dbt_utils.generate_surrogate_key(['dot_number',
'carrier_operation',
'legal_name',
'mcs150_date']) }} as  census_id,
{{ dbt_utils.generate_surrogate_key(['dot_number',
'carrier_operation',
'legal_name']) }} as  vehicle_detail_id,
 {{ dbt_utils.generate_surrogate_key(['phy_street',
'phy_city',
'phy_state',
'phy_zip',
'phy_country',
'mailing_street',
'mailing_city',
'mailing_state',
'mailing_zip']) }} as  address_id,
 {{ dbt_utils.generate_surrogate_key(['email_address',
'telephone',
'fax']) }} as  contact_id,
recent_mileage,
mcs150_mileage,
nbr_power_unit,
driver_total
from data