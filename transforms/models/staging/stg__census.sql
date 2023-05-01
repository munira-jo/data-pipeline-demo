with data as (
    select *
    from {{source('staging','census')}}
)

select 

--strings

--identifying info
dot_number,
legal_name,
dba_name,
carrier_operation,
oic_state,


--flags
hm_flag,
pc_flag,
vmt_source_id,

--physical_address
phy_street,
phy_city,
phy_state,
phy_zip,
phy_country,


--mailing_address
mailing_street,
mailing_city,
mailing_state,
mailing_zip,
mailing_country,

--contact
telephone,
fax,
email_address,


--dates
add_date,
mcs150_date,

--numbers
mcs150_mileage,
mcs150_mileage_year,
nbr_power_unit,
driver_total,
recent_mileage,
recent_mileage_year

from data