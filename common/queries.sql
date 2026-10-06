-- Dataset Summaries for Metrics

-- Cluster observation by time. Were you expecting many small clusters or a few large or ???
select
--# name: observation_times
time_bucket(INTERVAL '15 minutes', observation_ts) ObsTime, count(*) NumRows
from observations group by all order by all
;

select
--# name: observation_count
count(*) observations from observations
;

select
--# name: location_count
count(distinct location_pk) locations from observations
;

select
--# name: signed_count
   count(*) Signed from sigs_n_certs where uuid is not null
;

-- Get raw data collection range, which means we need to ignore process data reconstructed from Windows logs, which are activitytype="refresh"
select
--# name: data_range
	min(observation_ts) first_seen,
	max(observation_ts) last_seen,
from
	observations
;

-- Summarize by features
select
--# name: feature_summary
   len(imphash)=32 Imphash, 
   magic is not null Magic, 
   authentihash is not null Authentihash, 
   s.uuid is not null Signed, 
   -- There can be more than 1 sig row due to multiple certs.
   count(distinct o.uuid) "Rows",
   -- Pyarrow doesn't support custom types(?) 
   cast(first(o.uuid) as varchar) Example,
   'debug_page?uuid='||Example ExampleURL
from observations o
left outer join sigs_n_certs s on o.uuid=s.uuid
-- Bad data? 
where o.uuid is not null
group by all 
order by "Rows" desc
;

-- Get filesizes
select 
--# name: file_sizes
bytecount FROM observations
;

-- Cluster and count file extensions
SELECT 
--# name: file_extensions
LOWER(SUBSTRING(REGEXP_EXTRACT(filename, '\.([^.]*)$'), 0)) file_extension, count(*) NumRows
FROM observations group by all order by NumRows DESC LIMIT 30
;

-- Cluster by magic type
-- Note that many magic types have specifics, like number of lines in ASCII text, so we just use the first 2 tokens of the magic
SELECT 
--# name: magic_type
    -- Split into tokens, then concat the first 2 back together
	concat_ws(' ',split(magic,' ')[1],split(magic,' ')[2]) magic_type,
	count(distinct magic) uniq_count,
	count(*) NumRows,
	first(magic) example
from
	observations
group by all
order by NumRows desc
limit 30
;

-- All batches
select
--# name: all_batches
  batch_id,
  location_pk,
  num_rows
from batches order by all
;

-- New Batches
select 
--# name: new_batches
* from new_batches
order by batch_id
;
