with rand_id as (
  select gen_no, uniform(0, 10000000000, random())/10000000000 as rand
  from (select seq4() as gen_no from table (generator(rowcount => :videos*3)))
)

   -- videos with views from the last 7d
   , v1 as (
  select *
       , sum(views) over (order by video_id rows unbounded preceding) as views_running
       , sum(views) over () as views_total
  from (
         select video_id, sum(views) as views
         from video_stats_daily
         where updated>dateadd(day, -7, (select max(updated) from video_stats_daily))
         group by 1
       )
)

   -- choose tag videos randomly weighted by their views
   , v2 as (
  select v1.*
       , rand*views_total as rand_views
       , coalesce(lag(views_running) over (partition by gen_no order by video_id), 0) as last_views_running
       , gen_no
  from rand_id
         left join v1
    qualify rand_views>last_views_running and rand_views<=views_running
)

   , s as (
  select v2.video_id
       , v.channel_title
       , v.channel_id
       , v.video_title
       , v.upload_date
       , v2.views as views_recent
       , v.views
  from v2
         inner join video_latest v on v2.video_id=v.video_id
         inner join channel_latest cl on v.channel_id=cl.channel_id
    qualify row_number() over (partition by v2.video_id order by v2.video_id)=1
  limit :videos
)
select *
from s