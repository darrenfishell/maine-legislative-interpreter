with bills as (
    select * from {{ ref('stg_bills') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ld_number', 'legislature']) }} as bill_key,
    ld_number,
    paper_number,
    legislature,
    item_number,
    request_id,
    document_id,
    request_item_type,
    title,
    summary,
    location,
    url
from bills
