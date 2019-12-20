with cr as (
    select channel_id
         , sum(relevant_impressions) as relevant_impressions
         , sum(relevant_impressions_in) as relevant_impressions_in
         , sum(relevant_impressions) / datediff(day, :from::date, last_day(:to::date, month)) as relevant_impressions_daily
         , sum(relevant_impressions_in) / datediff(day, :from::date, last_day(:to::date, month)) as relevant_impressions_in_daily
         , max(meets_subsviews_criteria) as meets_subsviews_criteria
    from channel_stats_monthly
    where rec_month between :from and last_day(:to::date, month)
    group by channel_id
)

select c.channel_id
     , channel_title
     , channel_views
     , country
     , relevance
     , subs
     , daily_views
     , relevant_daily_views
     , views
     , relevant_impressions_daily
     , relevant_impressions_in_daily
     , avg_minutes
     , from_date
     , to_date
     , lr
     , tags
     , ideology
     , media
     , manoel
     , ain
     , logo_url
from channel_latest c
         left join cr on c.channel_id = cr.channel_id
where cr.meets_subsviews_criteria = true