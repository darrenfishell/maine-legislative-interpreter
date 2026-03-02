with sessions as (
    select distinct legislature
    from {{ ref('stg_bills') }}

    union

    select distinct legislature
    from {{ ref('stg_testimony') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['legislature']) }} as session_key,
    legislature                                              as legislature_number,
    legislature || case
        when legislature % 10 = 1 and legislature % 100 != 11 then 'st'
        when legislature % 10 = 2 and legislature % 100 != 12 then 'nd'
        when legislature % 10 = 3 and legislature % 100 != 13 then 'rd'
        else 'th'
    end || ' Legislature'                                    as session_label
from sessions
