{% macro safe_silver_source(identifier) -%}
  {#
    Return a Relation for silver.<identifier> if it exists, else return none.
    
    This is used to make staging models resilient when curated metadata types
    have no rows yet (DLT won't have created the underlying silver table).
  #}
  {{ return(adapter.get_relation(database=target.database, schema='silver', identifier=identifier)) }}
{%- endmacro %}
