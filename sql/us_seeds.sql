-- videos with view rank within a channel in the last year
with c_rand as (
  select channel_id, gen_no, uniform(0, 10000000000, random())/10000000000 as rand
  from (select seq4() as gen_no from table (generator(rowcount => :videos_per_tag / 10)))
     , (select distinct channel_id from channel_accepted) c
)

   -- videos with running total, used to select videos at random proportionally to views
   , v1 as (
  select c.channel_title
       , c.channel_id
       , c.tags
       , video_id
       , video_title
       , views
       , rank() over (partition by c.channel_id order by views desc) as video_rank_in_channel
       , sum(views) over (partition by c.channel_id order by views rows unbounded preceding) as views_running
       , sum(views) over (partition by c.channel_id) as channel_views_total
  from video_latest v
         left join channel_accepted c on v.channel_id=c.channel_id
  where c.channel_id is not null
    and views>0
    and upload_date>dateadd(day, -365, (select max(upload_date) from video_latest))
)

   -- v1 + last running_views
   , v2 as (
  select v1.*
       , r.gen_no
       , r.rand*channel_views_total as rand_views
       , coalesce(lag(views_running) over (partition by v1.channel_id, gen_no order by views), 0) as last_views_running
  from v1
         left join c_rand r on v1.channel_id=r.channel_id
    qualify
      rand_views>last_views_running and rand_views<=views_running -- chosen randomly proportional to views
)

   , v3 as (
  -- distinct videos
  select *
       , row_number() over (partition by video_id order by gen_no) as video_id_no
  from v2
    qualify video_id_no=1
)
   , v4 as (
  select tag
       , row_number() over (partition by tag order by gen_no desc) as ideology_rank -- arbitrarily chose x within and idelogy
       , v3.*
  from us_tags t
         left join v3 on array_contains(t.tag::variant, v3.tags)
    qualify ideology_rank <= :videos_per_tag
)
select *
from v4
order by tag, video_id