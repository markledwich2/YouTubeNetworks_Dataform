
-- percent of non-political videos for anon vs any account
with raw as (
    select f.video_id,
           f.updated,
           v.channel_id,
           account,
           c.tags is not null and array_size(c.tags) > 0 political,
           iff(account = 'Fresh', 'Fresh', 'Persona')        persona
    from us_feed f
             left join video_latest v on f.video_id = v.video_id
             left join channel_latest c on v.channel_id = c.channel_id
    where f.account not in ('Black'))
select persona, count(*), count_if(political) / count(*) non_political
from raw
group by 1;

-- percent of an accounts home page video's from channels they already had watched
with feed as (
    select f.video_id, f.updated, vl.channel_id, account
    from us_feed f
             left join video_latest vl on f.video_id = vl.video_id
    where f.account not in ('Black', 'Fresh')),
     account_channel_watch as (
         select channel_id, w.account, min(w.updated) as first_watched
         from us_watch w
                  left join video_latest v on w.video_id = v.video_id
         group by 1, 2)
        ,
     raw_recs as (
         select f.account,
                f.channel_id,
                c.channel_title,
                f.video_id,
                c.tags,
                w.first_watched,
                f.updated,
                coalesce(w.first_watched < f.updated, false) as           channel_watched,
                coalesce(array_contains(f.account::variant, tags), false) intra_category
         from feed f
                  left join account_channel_watch w on w.account = f.account and w.channel_id = f.channel_id
                  left join channel_latest c on w.channel_id = c.channel_id),
     all_recs as (
         select count_if(intra_category) / count(*)  intra_category,
                count_if(channel_watched) / count(*) pct_channel_watched
         from raw_recs f),
     account_recs as (
         select f.account,
                count_if(intra_category) / count(*)  intra_category,
                count_if(channel_watched) / count(*) pct_channel_watched
         from raw_recs f
         group by f.account)
select *
from account_recs
--where not intra_category and channel_watched and account = 'WhiteIdentitarian'
;

-- percent of accounts video recommendations from channels they have already watched
with account_channel_watch as (
    select channel_id, w.account, min(w.updated) as first_watched
    from us_watch w
             left join video_latest v on w.video_id = v.video_id
    where w.account not in ('Black')
    group by 1, 2),
     all_recs as (
         select count(*),
                count_if(array_contains(r.account::variant, to_chan.tags)) / count(*)          intra_category,
                count_if(r.to_channel_id = from_channel_id) / count(*)                         self_recs,
                count_if(w.first_watched < r.updated) / count(*)                               channel_watched_recs,
                count_if(r.to_channel_id <> from_channel_id and
                         (w.first_watched is null or w.first_watched >= r.updated)) / count(*) novel_recs
         from us_rec r
                  left join account_channel_watch w on w.account = r.account and w.channel_id = r.to_channel_id
                  left join channel_latest to_chan on r.to_channel_id = to_chan.channel_id
         where r.account not in ('Fresh', 'Black')),
     grouped_recs as (
         select r.account,
                count(*),
                count_if(array_contains(r.account::variant, to_chan.tags)) / count(*)          intra_category,
                count_if(r.to_channel_id = from_channel_id) / count(*)                         self_recs,
                count_if(w.first_watched < r.updated) / count(*)                               channel_watched_recs,
                count_if(r.to_channel_id <> from_channel_id and
                         (w.first_watched is null or w.first_watched >= r.updated)) / count(*) novel_recs
         from us_rec r
                  left join account_channel_watch w on w.account = r.account and w.channel_id = r.to_channel_id
                  left join channel_latest to_chan on r.to_channel_id = to_chan.channel_id
         group by 1)
select *
from all_recs