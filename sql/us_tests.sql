with
   -- random numbers per ideology
  rand_id as (
    select tag, gen_no, uniform(0, 10000000000, random())/10000000000 as rand
    from (select distinct tag from us_tags) t
      , (select seq4() as gen_no from table (generator(rowcount => :videos_per_tag)))
  )

   -- recent videos,
   , v1 as (
  select video_id
       , video_title
       , c.channel_id
       , c.channel_title
       , views
       , t.tag
       , sum(views) over (partition by tag order by views rows unbounded preceding) as views_running
       , sum(views) over (partition by tag) as views_total
  from video_latest v
         inner join channel_accepted c on v.channel_id=c.channel_id
         left join us_tags t on array_contains(t.tag::variant, c.tags) // row for each UserScrape tag
  where upload_date>dateadd(day, -90, (select max(upload_date) from video_latest))
)

   -- choose tag videos randomly weighted by their views
   , v2 as (
  select v1.*
       , rand*views_total as rand_views
       , coalesce(lag(views_running) over (partition by v1.tag, gen_no order by views), 0) as last_views_running
       , gen_no
  from v1
         left join rand_id on v1.tag=rand_id.tag
    qualify rand_views>last_views_running and rand_views<=views_running
)

select video_id, video_title, channel_id, channel_title, tag
from v2
order by tag