with subjects as (
    select distinct
        subject_name,
        subject_level
    from {{ ref('int_bill_subjects') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['subject_name', 'subject_level']) }} as subject_key,
    subject_name,
    subject_level
from subjects
