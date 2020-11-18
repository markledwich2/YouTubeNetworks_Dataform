with
  -- recs within the period
  period_recs as (
    select from_video_id
         , to_video_id
         , any_value(from_channel_id) from_channel_id
         , any_value(to_channel_id) to_channel_id
         , count(*) recs
    from rec r
           inner join channel_accepted fc on fc.channel_id=r.from_channel_id -- inner to filter only to accepted channels
    where r.updated::date between :from_date and dateadd(day, 7, :to_date) -- find recs up to 2 days past period we use video's from
    group by 1, 2
  )
  --portion of videos recommended from each channel in this period
   , default_channel_recs as (
  select *
  from (
         select from_channel_id, to_video_id, sum(recs) recs
         from period_recs
         group by 1, 2
       )
    qualify dense_rank() over (partition by from_channel_id order by recs desc)<100
)

   , default_support_portions as (
  select *
  from (
         select nf.support from_support, r.to_video_id, sum(recs) recs
         from period_recs r
                left join video_narrative nf on nf.video_id=r.from_video_id
         group by 1, 2
       )
    qualify dense_rank() over (partition by from_support order by recs desc)<100
)

   , period_views as (
  select video_id, channel_id, sum(views) views
  from video_stats_daily d
  where date between :from_date and :to_date
    -- and channel_id='UCeY0bbntWzzVIaj2z3QigXg'
    --and video_id='Tn8mN8MvrSI'
  group by 1, 2
)

  -- starting from all videos in the period, join with recs or fallback to default channel recs
   , video_recs as (
  select d.video_id from_video_id
       , coalesce(pr.to_video_id, cr.to_video_id) to_video_id

       , any_value(d.channel_id) from_channel_id

       -- sum of recs, either from a real rec or the default channel recs
       , sum(coalesce(pr.recs, cr.recs, dr.recs)) recs_raw

       -- total recs from this video
       , sum(recs_raw) over (partition by d.video_id) from_video_recs

       -- views * by the portion recs to the video compared to the total number of recs
       , min(d.views)*(recs_raw/nullif(from_video_recs, 0)) as rec_view_portion

       -- estimate for the times a rec is seen. Assume 10 is the average number of recs visible form a video. so the impressions is 10*the portion of recs
       , rec_view_portion*10 as impressions
  from period_views d
         left join period_recs pr on pr.from_video_id=d.video_id
         left join default_channel_recs cr on pr.from_video_id is null and cr.from_channel_id=d.channel_id
         left join video_narrative n on n.video_id=d.video_id
         left join default_support_portions dr on cr.from_channel_id is null and dr.from_support=n.support
  group by d.video_id, pr.to_video_id, cr.to_video_id
)
-- impressions coming or going to narrative videos within this period
   , narrative_recs as (
  select nf.support from_support
       , nt.support to_support
       , sum(impressions) impressions
  from video_recs r
         left join video_narrative nf on nf.video_id=r.from_video_id
         left join video_narrative nt on nt.video_id=r.to_video_id
       //inner join video_stats_daily d on d.video_id=r.from_video_id and d.date=r.rec_date
  where (nt.video_id is not null or nf.video_id is not null)
  group by 1, 2
)
   , test_narrative_views as (
  select n.support, sum(d.views) views
  from period_views d
         inner join video_narrative n on n.video_id=d.video_id
  group by 1
)
   , test_from_support as (
  select from_support, impressions, views, impressions/views
  from (
         select from_support, sum(impressions) impressions
         from narrative_recs n
         group by from_support
       ) n
         left join test_narrative_views v on n.from_support=v.support
)

select *
from narrative_recs