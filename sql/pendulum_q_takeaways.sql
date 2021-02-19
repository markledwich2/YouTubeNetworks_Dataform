with platform_daily as (
  select date
       , platform
       , case
           when date between '2020-10-14' and dateadd('day', 30, '2020-10-14') then 'post-ban-30d'
           when date between dateadd('day', -30, '2020-10-13') and '2020-10-13'  then 'pre-ban-30d'
         end era
       , sum(views)/1000000 views
  from video_stats_daily v
         join channel_latest c on v.channel_id=c.channel_id
  where array_contains('QAnon'::variant,tags)
  group by 1, 2, 3
)
  , daily as (
  select date, sum(views) views
  from platform_daily
  group by 1
)
  , era as (
  select era, platform, avg(views)
  from platform_daily
  where platform='YouTube'
  group by 1, 2
)
  , jan_totes as (
  select avg(views)
  from daily
  where trunc(date,'month')='2021-01-01'
)
select *
from jan_totes