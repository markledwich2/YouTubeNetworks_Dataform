-- vis channel recs: significant recommendations the given range
with
     vis_channels as (
       select distinct channel_id from channel_stats_monthly
       where meets_subsviews_criteria and meets_review_criteria
     )

     ,r1 as (
  select from_channel_id
       , to_channel_id
       , sum(relevant_impressions) as relevant_impressions
       , sum(relevant_impressions_in) as relevant_impressions_in
       , max(datediff(day, :from::date, least(last_day(:to::date, month), current_date()))) as days
       , sum(relevant_impressions)/days as relevant_impressions_daily
  from channel_recs_monthly
  where exists(select * from vis_channels where channel_id = to_channel_id)
    and exists(select * from vis_channels where channel_id = from_channel_id)
    and rec_month between :from::date and :to::date
  group by from_channel_id, to_channel_id
)
   , r2 as (
  select *
       , rank() over (partition by from_channel_id order by relevant_impressions desc) as from_rank
       , rank() over (partition by from_channel_id order by relevant_impressions_in desc) as to_rank
       , relevant_impressions/
         nullif(sum(relevant_impressions) over (partition by from_channel_id), 0) as percent_of_channel_recs
  from r1
)
   , s as (
  select from_channel_id, to_channel_id, relevant_impressions_daily, percent_of_channel_recs
  from r2
  where relevant_impressions_daily>0
    and (from_rank<=10 or to_rank<=10 or percent_of_channel_recs>0.01)
)
select * from s