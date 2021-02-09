with file_lists as (
  select $1::string channel_id
       , regexp_substr(metadata$filename,'([\\w.]+)\.txt',1,1,'e') list
  from @public.yt_data/import/review_lists (file_format => tsv)
)
  , popular_under_reviewed as (
  select channel_id, 'Top viewed under-human-reviewed' list
  from channel_latest
  where reviews_human<1
    and meets_sub_criteria
  order by channel_views desc
  limit :limit
)
  , whide_id_under_reviewed as (
  select channel_id
       , concat(array_to_string(
                    array_intersection(array_construct('WhiteIdentitarian','QAnon'),c.tags),'|')
    ,' not-reviewed - '
    ,mod(abs(hash(c.channel_id)),4)) list
  from channel_accepted c
  where arrays_overlap(array_construct('WhiteIdentitarian','QAnon'),c.tags)
    and reviews_human<1
)
  , top_alts as (
  select channel_id
       , concat('Most popular on ',platform,' sans-review - ',mod(abs(hash(c.channel_id)),2)) list
  from channel_latest c
  where reviews_human<1
    and platform<>'YouTube'
  order by channel_video_views desc
)
  , u as (
  select *
  from file_lists
  union all
  select *
  from popular_under_reviewed
  union all
  select *
  from whide_id_under_reviewed
  union all
  select *
  from top_alts
)
  , s as (
  select u.channel_id
       , u.list
       , channel_title
       , description
       , logo_url
       , reviews_algo
       , reviews_all
       , channel_views
       , channel_video_views
       , platform
  from u
         left join channel_latest c on c.channel_id=u.channel_id
)
select *
from s