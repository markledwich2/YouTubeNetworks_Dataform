--use some social blade imported data to re-estimate video views
-- not in use. But saving because it might be usefull

with sbraw as (
  select $1:channelId::string channel_id, $1:totalViews::array series from @yt_data/import/socialblade/daily_series (file_format => json) d
)
  , sb1 as (
  select channel_id
       , dateadd('ms',s.value[0],'1970-01-01'::timestamp_ntz)::date date
       , s.value[1]::int raw_views
       , greatest(raw_views-coalesce(lag(raw_views) over (partition by channel_id order by date),0),0) delta
  from sbraw r
    , lateral flatten(input => series) s
)
  , sb2 as (
  select channel_id
       , date
       , delta
       , sum(delta) over (partition by channel_id order by date rows between unbounded preceding and current row) views
  from sb1
)
   -- proportion view from the channel daily stats.
   -- note: this means that the video totals won't match, but that the daily totals across video will
  , video_pct as (
  select d.channel_id
       , video_id
       , d.date
       , ratio_to_report(d.views) over (partition by d.channel_id, d.date) views_pct
       , s.delta channel_views
       , d.views original_views
       , views_pct*channel_views views
  from video_stats_daily d
         join sb2 s on s.channel_id=d.channel_id and s.date=d.date
)
, total_check as (
  select channel_id, sum(views)/1000000, sum(original_views)/1000000
from video_pct
group by channel_id
)
select * from video_pct
order by channel_id, video_id, date