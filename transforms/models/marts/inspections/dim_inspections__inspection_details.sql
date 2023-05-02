with data as (
    select distinct insp_level_id,
    insp_date,
    unsafe_insp,
    fatigued_insp,
    dr_fitness_insp,
    subt_alcohol_insp,
    vh_maint_insp,
    hm_insp,
    hazmat_placard_req
    from {{ref('stg__inspections')}}
)

select {{ dbt_utils.generate_surrogate_key(['insp_level_id',
'insp_date',
'hazmat_placard_req'
]) }} as  inspection_detail_id,
insp_level_id,
insp_date,
unsafe_insp,
fatigued_insp,
dr_fitness_insp,
subt_alcohol_insp,
vh_maint_insp,
hm_insp,
hazmat_placard_req
from data
