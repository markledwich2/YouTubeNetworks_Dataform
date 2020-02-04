select from_date
     , from_channel_hash
     , to_channel_hash
     , impressions_actual
     , impressions_estimate
     , from_ideology
     , to_ideology
from rec_export_vs_estimate
where from_date >= '2019-11-01'::date
order by from_date, to_channel_hash, from_channel_hash