with cr as (
    select channel_id
         , sum(relevant_impressions) as relevant_impressions
         , sum(relevant_impressions_in) as relevant_impressions_in
         , max(datediff(day, :from::date, least(last_day(:to::date, month), current_date()))) as days
         , sum(relevant_impressions) / days as relevant_impressions_daily
         , sum(relevant_impressions_in) / days as relevant_impressions_in_daily
         , max(meets_subsviews_criteria) as meets_subsviews_criteria
         , sum(video_views) / days as video_views_daily
         , sum(relevant_video_views) / days as relevant_video_views_daily
    from channel_stats_monthly
    where month between :from and last_day(:to::date, month)
    group by channel_id
)
, s as (
  select c.channel_id
       , channel_title
       , channel_views
       , country
       , relevance
       , subs
       , channel_lifetime_daily_views
       , channel_lifetime_daily_views_relevant
       , channel_video_views
       , relevant_impressions_daily
       , relevant_impressions_in_daily
       , video_views_daily
       , relevant_video_views_daily
       , avg_minutes
       , from_date
       , to_date
       , lr
       , tags
       , ideology
       , media
       , logo_url
  from channel_accepted c
         left join cr on c.channel_id=cr.channel_id
  where cr.meets_subsviews_criteria and c.platform = 'YouTube'
)
select * from s