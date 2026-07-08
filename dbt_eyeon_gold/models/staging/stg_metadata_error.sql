{% set rel = safe_silver_source('metadata_error') %}

{% if rel is none %}
select
  cast(null as varchar) as uuid,
  cast(null as varchar) as _metadata_table_name
where false
{% else %}
select * from {{ rel }}
{% endif %}
