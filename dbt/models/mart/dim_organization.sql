with org_clusters as (
    select * from {{ ref('int_org_clusters') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['organization_name']) }} as org_key,
    organization_name,
    standard_org_name,
    testimony_count
from org_clusters
