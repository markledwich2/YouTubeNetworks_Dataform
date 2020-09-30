with rand_id as (
  select gen_no, uniform(0, 10000000000, random())/10000000000 as rand
  from (select seq4() as gen_no from table (generator(rowcount => :videos*3)))
)
  -- videos with views from the last 7d
   , recent_videos as (
  select *
       , sum(views) over (order by video_id rows unbounded preceding) as views_running
       , sum(views) over () as views_total
  from (
         select video_id, sum(views) as views
         from video_stats_daily d
         where updated>dateadd(day, -7, (select max(updated) from video_stats_daily))
           and exists(select * from channel_accepted c where c.channel_id=d.channel_id)
         group by 1
       )
)
  -- choose tag videos randomly weighted by their views
   , random_recent_videos as (
  select recent_videos.*
       , rand*views_total as rand_views
       , coalesce(lag(views_running) over (partition by gen_no order by video_id), 0) as last_views_running
       , gen_no
  from rand_id
         left join recent_videos
    qualify rand_views>last_views_running and rand_views<=views_running
)
   , top_recent_videos as (
  select video_id
  from random_recent_videos r
    qualify row_number() over (partition by video_id order by video_id)=1
  limit :videos
)
   , manual_raw as (
  select $1::string video_id
       , $2::string label
       , $3::date expires
  from @public.yt_data/import/us_explicit_tests.tsv (file_format => tsv_header)
)
   , manual as (
  select r.video_id, r.label
  from manual_raw r
         left join video_latest v on r.video_id=v.video_id
  where expires>=current_date()
)
   , random_and_manual as (
  select video_id, label
  from manual
  union all
  select video_id, 'random' as label
  from top_recent_videos
)
   , s as (
  select m.video_id
       , m.label
       , v.channel_title
       , v.channel_id
       , v.video_title
       , v.upload_date
       , v.views
  from random_and_manual m
         inner join video_latest v on m.video_id=v.video_id
)
select *
from s