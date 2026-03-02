with testimony as (
    select * from {{ ref('stg_testimony') }}
),

distinct_persons as (
    select distinct
        name_prefix,
        first_name,
        last_name,
        name_suffix
    from testimony
    where first_name is not null
       or last_name is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'name_suffix']) }} as person_key,
    name_prefix,
    first_name,
    last_name,
    name_suffix
from distinct_persons
