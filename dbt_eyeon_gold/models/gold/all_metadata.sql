{{ config(materialized='view') }}
select uuid, _metadata_table_name from {{ ref('stg_metadata_binwalk_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_coff_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_container_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_device_tree_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_elf_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_error') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_generic_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_java_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_js_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_mach_o_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_native_lib') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_ole_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_opkg_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_pe_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_symlink_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_text_file') }}
union all
select uuid, _metadata_table_name from {{ ref('stg_metadata_uimage_file') }}
