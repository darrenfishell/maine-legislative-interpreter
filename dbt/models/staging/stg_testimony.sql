with source as (
    select * from {{ source('raw', 'testimony_attributes') }}
),

renamed as (
    select
        cast(id as integer)                        as testimony_id,
        cast(request_id as integer)                as request_id,
        source_document,
        file_type,
        trim(file_size)                            as file_size,
        trim(nullif(name_prefix, ''))             as name_prefix,
        trim(first_name)                           as first_name,
        trim(last_name)                            as last_name,
        trim(nullif(name_suffix, ''))             as name_suffix,
        trim(nullif(organization, ''))            as organization,
        try_cast(presented_date as date)          as presented_date,
        trim(nullif(policy_area, ''))             as policy_area,
        trim(nullif(topic, ''))                   as topic,
        try_cast(created as timestamp)            as created_at,
        created_by,
        try_cast(last_edited as timestamp)        as last_edited_at,
        last_edited_by,
        cast(private as boolean)                  as is_private,
        cast(inactive as boolean)                 as is_inactive,
        testimony_submission_id,
        try_cast(hearing_date as date)           as hearing_date,
        cast(ld_number as integer)               as ld_number,
        cast(legislature as integer)             as legislature
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
