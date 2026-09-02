-- Feature Summary
select
--# name: sig_feature_summary
  basic_constraints like 'CA=true%' is_ca,
  key_usage,
  ext_key_usage,
  count(*) "Rows",
   -- Pyarrow doesn't support custom types(?) 
   cast(first(uuid) as varchar) Example
from sigs_n_certs
group by all 
order by "Rows" desc
;

-- Summarize by location
select
--# name: cert_locations
  o.location_pk "Location", count(*) NumRows
from observations o
join sigs_n_certs s on s.uuid=o.uuid
where o.uuid is not null
group by all order by all
;


-- Count different RSA Key sizes
select
--# name: rsa_key_sizes
RSA_key_size, count(*) NumKeys from raw_uniq_certs group by all order by all
;

-- Cluster cert expiration times by year
select
--# name: expiration_years
time_bucket(INTERVAL '1 year', expires_on) ExpiryYear, count(*) "Expiring Certs"
from raw_uniq_certs group by all order by all
;

-- Cluster cert issued_on time by year
select
--# name: issue_years
time_bucket(INTERVAL '1 year', issued_on) IssueYear, count(*) "Issued Certs"
from raw_uniq_certs group by all order by all
;

-- Gets the state from the subject name
select 
--# name: subject_states
SUBSTRING(REGEXP_EXTRACT(subject_name, 'ST=([^,]+)'), 4) State, count(*) NumRows
FROM raw_uniq_certs group by all order by NumRows DESC
;

-- Gets the organization from the subject name
select 
--# name: organizations
SUBSTRING(REGEXP_EXTRACT(subject_name, 'O=([^,]+)'), 3) State, count(*) NumRows
FROM raw_uniq_certs group by all order by NumRows DESC
;
