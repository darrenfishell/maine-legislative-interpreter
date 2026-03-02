with embeddings as (
    select * from {{ source('intermediate_dlt', 'int_sentence_embeddings') }}
),

testimony as (
    select * from {{ ref('stg_testimony') }}
),

dim_bill as (
    select * from {{ ref('dim_bill') }}
),

dim_session as (
    select * from {{ ref('dim_session') }}
)

select
    e.doc_id,
    e.sentence_index,
    e.sentence                  as sentence_text,
    e.embedding,
    e.model_name,
    e.embedding_dimension,
    b.bill_key,
    s.session_key
from embeddings e
left join testimony t
    on e.doc_id = t.testimony_id
left join dim_bill b
    on t.ld_number = b.ld_number
    and t.legislature = b.legislature
left join dim_session s
    on t.legislature = s.legislature_number
