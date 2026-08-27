-- Queries on Metadata


select
--# name: md_feature_summary
   os, elfHumanArch, peMachine, count(*) "Rows",
   -- Pyarrow doesn't support custom types(?) 
   cast(first(uuid) as varchar) Example
from metadata 
group by all 
order by all
;