{{ config(materialized='view') }}

-- Change detection between consecutive batches of the same utility.
-- One row per (batch, sha256, change_type):
--   'new'         — hash never seen in any earlier batch of this utility
--   'disappeared' — hash present in the immediately preceding batch, absent now
-- A utility's first batch is the baseline and produces no 'new' rows.

with presence as (
    select distinct
        b.utility_id,
        o._dlt_load_id,
        b.run_ts,
        o.sha256
    from {{ ref('stg_raw_obs') }} o
    join {{ ref('stg_batch_info') }} b on b._dlt_load_id = o._dlt_load_id
    where o.sha256 is not null
),

batches as (
    select
        utility_id,
        _dlt_load_id,
        run_ts,
        dense_rank() over (
            partition by utility_id order by run_ts, _dlt_load_id
        ) as batch_seq
    from (select distinct utility_id, _dlt_load_id, run_ts from presence)
),

ranked as (
    select p.utility_id, p._dlt_load_id, p.run_ts, p.sha256, b.batch_seq
    from presence p
    join batches b
      on b.utility_id = p.utility_id
     and b._dlt_load_id = p._dlt_load_id
),

new_files as (
    select
        r.utility_id,
        r._dlt_load_id,
        r.run_ts,
        r.batch_seq,
        r.sha256,
        'new' as change_type
    from ranked r
    where r.batch_seq > 1
      and not exists (
        select 1 from ranked prior
        where prior.utility_id = r.utility_id
          and prior.sha256 = r.sha256
          and prior.batch_seq < r.batch_seq
      )
),

disappeared as (
    select
        prev.utility_id,
        nxt._dlt_load_id,
        nxt.run_ts,
        nxt.batch_seq,
        prev.sha256,
        'disappeared' as change_type
    from ranked prev
    join batches nxt
      on nxt.utility_id = prev.utility_id
     and nxt.batch_seq = prev.batch_seq + 1
    where not exists (
        select 1 from ranked cur
        where cur.utility_id = prev.utility_id
          and cur.sha256 = prev.sha256
          and cur.batch_seq = prev.batch_seq + 1
      )
)

select * from new_files
union all
select * from disappeared
