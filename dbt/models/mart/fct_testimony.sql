with testimony as (
    select * from {{ ref('stg_testimony') }}
),

positions as (
    select * from {{ ref('int_testimony_position') }}
),

dim_bill as (
    select * from {{ ref('dim_bill') }}
),

dim_person as (
    select * from {{ ref('dim_person') }}
),

dim_org as (
    select * from {{ ref('dim_organization') }}
),

dim_session as (
    select * from {{ ref('dim_session') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['t.testimony_id']) }} as testimony_key,
    t.testimony_id,
    b.bill_key,
    p.person_key,
    o.org_key,
    s.session_key,
    dh.date_key            as hearing_date_key,
    dp.date_key            as presented_date_key,
    t.file_type,
    t.file_size,
    pos.text_length,
    coalesce(pos.position, 'unknown') as position,
    t.policy_area,
    t.topic
from testimony t
left join positions pos
    on t.testimony_id = pos.testimony_id
left join dim_bill b
    on t.ld_number = b.ld_number
    and t.legislature = b.legislature
left join dim_person p
    on coalesce(t.first_name, '') = coalesce(p.first_name, '')
    and coalesce(t.last_name, '') = coalesce(p.last_name, '')
    and coalesce(t.name_suffix, '') = coalesce(p.name_suffix, '')
left join dim_org o
    on t.organization = o.organization_name
left join dim_session s
    on t.legislature = s.legislature_number
left join dim_date dh
    on t.hearing_date = dh.full_date
left join dim_date dp
    on t.presented_date = dp.full_date
