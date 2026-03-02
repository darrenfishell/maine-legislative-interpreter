with source as (
    select * from {{ source('raw', 'bill_text') }}
),

renamed as (
    select
        "ldNumber"                          as ld_number,
        "paperNumber"                       as paper_number,
        cast("legislature" as integer)      as legislature,
        cast("itemNumber" as integer)       as item_number,
        "requestId"                         as request_id,
        "documentId"                        as document_id,
        "requestItemType"                   as request_item_type,
        trim("title")                       as title,
        trim("summary")                     as summary,
        trim("location")                    as location,
        trim(nullif("broadSubject", ''))    as broad_subject,
        trim(nullif("majorSubject", ''))    as major_subject,
        trim(nullif("minorSubject", ''))    as minor_subject,
        trim(nullif("detailSubject", ''))   as detail_subject,
        "url"                               as url,
        "ordering"                          as ordering
    from source
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
