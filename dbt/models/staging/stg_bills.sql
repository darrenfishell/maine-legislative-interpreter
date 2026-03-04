with src as (
    select * from {{ source('raw', 'bill_text') }}
),

renamed as (
    select
        ld_number,
        paper_number,
        cast(legislature as integer)                 as legislature,
        cast(item_number as integer)                 as item_number,
        request_id,
        document_id,
        request_item_type,
        trim(title)                                  as title,
        trim(summary)                                as summary,
        trim(cast(location as varchar))              as location,
        cast(null as varchar)                        as broad_subject,
        cast(null as varchar)                        as major_subject,
        cast(null as varchar)                        as minor_subject,
        cast(null as varchar)                        as detail_subject,
        url,
        ordering
    from src
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by ld_number, legislature, item_number
            order by ordering desc
        ) as _row_num
    from renamed
)

select * exclude (_row_num)
from deduplicated
where _row_num = 1
