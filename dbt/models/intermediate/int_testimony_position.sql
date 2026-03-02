with cleaned_text as (
    select * from {{ source('staging_dlt', 'stg_testimony_cleaned_text') }}
),

testimony as (
    select * from {{ ref('stg_testimony') }}
),

classified as (
    select
        ct.doc_id,
        ct.session           as legislature,
        ct.cleaned_text,
        case
            when lower(ct.cleaned_text) like '%neither for nor against%'
                then 'neither'
            when lower(ct.cleaned_text) like '%in opposition to%'
                 and lower(ct.cleaned_text) like '%in support of%'
                then 'mixed'
            when lower(ct.cleaned_text) like '%in opposition to%'
                then 'oppose'
            when lower(ct.cleaned_text) like '%in support of%'
                then 'support'
            when lower(ct.cleaned_text) like '%opposed%'
                then 'oppose'
            when lower(ct.cleaned_text) like '%in favor%'
                then 'support'
            else 'unknown'
        end as position
    from cleaned_text ct
)

select
    c.doc_id,
    c.legislature,
    c.position,
    length(c.cleaned_text) as text_length,
    t.testimony_id,
    t.ld_number,
    t.hearing_date,
    t.presented_date
from classified c
left join testimony t
    on c.doc_id = t.testimony_id
