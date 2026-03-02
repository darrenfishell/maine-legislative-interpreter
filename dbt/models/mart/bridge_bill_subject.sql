with bill_subjects as (
    select * from {{ ref('int_bill_subjects') }}
),

dim_bill as (
    select * from {{ ref('dim_bill') }}
),

dim_subject as (
    select * from {{ ref('dim_subject') }}
)

select
    b.bill_key,
    s.subject_key,
    bs.subject_level
from bill_subjects bs
inner join dim_bill b
    on bs.ld_number = b.ld_number
    and bs.legislature = b.legislature
inner join dim_subject s
    on bs.subject_name = s.subject_name
    and bs.subject_level = s.subject_level
