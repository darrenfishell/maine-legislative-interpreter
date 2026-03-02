with source as (
    select * from {{ source('raw', 'testimony_attributes') }}
),

renamed as (
    select
        cast("Id" as integer)                       as testimony_id,
        cast("RequestId" as integer)                 as request_id,
        "SourceDocument"                             as source_document,
        "FileType"                                   as file_type,
        cast("FileSize" as integer)                  as file_size,
        trim(nullif("NamePrefix", ''))               as name_prefix,
        trim("FirstName")                            as first_name,
        trim("LastName")                             as last_name,
        trim(nullif("NameSuffix", ''))               as name_suffix,
        trim(nullif("Organization", ''))             as organization,
        try_cast("PresentedDate" as date)            as presented_date,
        trim(nullif("PolicyArea", ''))               as policy_area,
        trim(nullif("Topic", ''))                    as topic,
        try_cast("Created" as timestamp)             as created_at,
        "CreatedBy"                                  as created_by,
        try_cast("LastEdited" as timestamp)          as last_edited_at,
        "LastEditedBy"                               as last_edited_by,
        cast("Private" as boolean)                   as is_private,
        cast("Inactive" as boolean)                  as is_inactive,
        "TestimonySubmissionId"                       as testimony_submission_id,
        try_cast("HearingDate" as date)              as hearing_date,
        cast("LDNumber" as integer)                  as ld_number,
        cast("legislature" as integer)               as legislature
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by testimony_id
            order by last_edited_at desc nulls last
        ) as _row_num
    from renamed
)

select * exclude (_row_num)
from deduplicated
where _row_num = 1
