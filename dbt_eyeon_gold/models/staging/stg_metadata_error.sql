select * from {{ source('silver', 'metadata_error') }}
