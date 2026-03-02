with date_spine as (
    select distinct hearing_date as full_date
    from {{ ref('stg_testimony') }}
    where hearing_date is not null

    union

    select distinct presented_date as full_date
    from {{ ref('stg_testimony') }}
    where presented_date is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_key,
    full_date,
    extract(year from full_date)    as year,
    extract(month from full_date)   as month,
    extract(day from full_date)     as day,
    dayname(full_date)              as day_of_week
from date_spine
