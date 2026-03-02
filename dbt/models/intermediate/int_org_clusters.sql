with organizations as (
    select distinct
        organization,
        legislature
    from {{ ref('stg_testimony') }}
    where organization is not null
),

org_frequency as (
    select
        organization,
        count(*) as testimony_count
    from {{ ref('stg_testimony') }}
    where organization is not null
    group by organization
)

select
    o.organization                  as organization_name,
    o.organization                  as standard_org_name,
    coalesce(f.testimony_count, 0)  as testimony_count
from (select distinct organization from organizations) o
left join org_frequency f
    on o.organization = f.organization
