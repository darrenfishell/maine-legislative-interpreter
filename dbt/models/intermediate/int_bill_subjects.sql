with bills as (
    select * from {{ ref('stg_bills') }}
),

unpivoted as (
    select ld_number, legislature, broad_subject as subject_name, 'broad' as subject_level
    from bills where broad_subject is not null

    union all

    select ld_number, legislature, major_subject as subject_name, 'major' as subject_level
    from bills where major_subject is not null

    union all

    select ld_number, legislature, minor_subject as subject_name, 'minor' as subject_level
    from bills where minor_subject is not null

    union all

    select ld_number, legislature, detail_subject as subject_name, 'detail' as subject_level
    from bills where detail_subject is not null
)

select distinct
    ld_number,
    legislature,
    subject_name,
    subject_level
from unpivoted
