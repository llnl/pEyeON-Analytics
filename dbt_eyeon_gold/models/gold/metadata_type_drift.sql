{{ config(materialized='view') }}

-- Discovered vs curated metadata types.
--
-- Silver (DLT) creates metadata tables dynamically based on whatever metadata keys
-- appear in the ingested EyeON JSON ("discovery").
-- Gold (dbt) curates a subset of those types by explicitly unioning known staging
-- models in `gold.all_metadata` ("modeled").
--
-- This view makes the gap explicit so the Streamlit UI can surface “unmodeled”
-- types (present in silver but missing from gold.all_metadata).

with discovered as (
  select
    table_name as metadata_table_name
  from information_schema.tables
  where table_schema = 'silver'
    -- DuckDB information_schema.tables uses LIKE with a default escape, so
    -- `metadata_%` treats `_` as a single-character wildcard. We want a literal
    -- underscore, so filter by prefix instead.
    and left(table_name, 9) = 'metadata_'
    -- DuckDB LIKE treats '_' as wildcard; avoid LIKE here and use substring search.
    and instr(table_name, '__') = 0
),

modeled as (
  -- Treat "modeled" as "dbt has an explicit staging model for this metadata
  -- type", independent of whether there are currently any rows in that table.
  -- (If we derive this from `gold.all_metadata` rows, empty-but-curated types
  -- incorrectly look unmodeled.)
  select distinct
    case
      -- Historical naming mismatch: model name drops `_file` but source table keeps it.
      when table_name = 'stg_metadata_native_lib' then 'metadata_native_lib_file'
      else replace(table_name, 'stg_', '')
    end as metadata_table_name
  from information_schema.tables
  where table_schema = 'gold_staging'
    and left(table_name, 13) = 'stg_metadata_'
)

select
  d.metadata_table_name,
  m.metadata_table_name is not null as is_modeled,
  case
    when m.metadata_table_name is not null then 'modeled'
    else 'unmodeled'
  end as status
from discovered d
left join modeled m using (metadata_table_name)
