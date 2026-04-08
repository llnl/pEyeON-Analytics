select * from {{ source('silver', 'metadata_coff_file') }}
