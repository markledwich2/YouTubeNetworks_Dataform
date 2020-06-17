with
   -- random numbers per ideology
  rand_id as (
    select ideology, gen_no, uniform(0, 10000000000, random()) / 10000000000 as rand
    from (select seq4() as gen_no from table (generator(rowcount => :videos_per_ideology)))
       , (select distinct ideology from channel_accepted) c
  )

   -- recent videos
   , v1 as (
  select video_id
       , video_title
       , c.channel_id
       , c.channel_title
       , views
       , c.ideology
       , sum(views) over (partition by ideology order by views rows unbounded preceding) as views_running
       , sum(views) over (partition by ideology) as views_total
  from video_latest v
         inner join channel_accepted c on v.channel_id = c.channel_id
  where upload_date > dateadd(day, -90, (select max(upload_date) from video_latest))
)

   -- choose ideology videos randomly weighted by their views
   , v3 as (
  select v1.*
       , rand * views_total as rand_views
       , coalesce(lag(views_running) over (partition by v1.ideology, gen_no order by views), 0) as last_views_running

       , gen_no
  from v1
         left join rand_id on v1.ideology = rand_id.ideology
    qualify rand_views > last_views_running and rand_views <= views_running
)

select *
from v3
order by ideology, views
