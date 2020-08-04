with r as (
    select sum(relevant_impressions /
               datediff(day, :from::date, least(last_day(:to::date, month), current_date()))) as relevant_impressions_daily,
           from_ideology,
           to_ideology,
           from_media,
           to_media,
           from_lr,
           to_lr
    from channel_recs_monthly
    where rec_month between :from::date and last_day(:to::date, month)
    group by 2,3,4,5,6,7
)
select *
from r