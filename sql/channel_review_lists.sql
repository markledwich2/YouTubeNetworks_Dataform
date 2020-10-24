with file_lists as (
  select $1::string as channel_id
  , regexp_substr(metadata$filename, '([\\w.]+)\.txt', 1, 1, 'e') as list
  from @public.yt_data/import/review_lists (file_format => tsv)
)
   , random_under_reviewed as (
  select *
  from (
         select channel_id, 'Random under-human-reviewed' as list
         from channel_latest
         where reviews_human<2
           and meets_sub_criteria
       )
         sample (:limit rows)
)
   , popular_under_reviewed as (
  select channel_id, 'Top viewed under-human-reviewed' as list
  from channel_latest
  where reviews_human<1
    and meets_sub_criteria
  order by channel_views desc
  limit :limit
)
   , small_tags as (
  select array_agg(tag) top_tags
  from (
         select f.value::string as tag, count(*) count
         from channel_latest
            , table ( flatten(tags) ) f
         where reviews_human>0
         group by tag
         order by count
        limit 8
       )
)
   , popular_non_reviewed_tag as (
  select channel_id, 'Top viewed algo identified under-human-reviewed tags (bottom 8 tags)' as list
  from channel_latest c
         join small_tags t on arrays_overlap(c.tags, t.top_tags)
  where reviews_algo>0
    and reviews_human=0
    and meets_sub_criteria
  order by channel_views desc
  limit :limit
)
  , whide_id_under_reviewed as (
     select channel_id
            , concat(array_to_string(
                array_intersection(array_construct('WhiteIdentitarian', 'QAnon'), c.tags), '|')
                , ' not-reviewed - '
                , mod(abs(hash(c.channel_id)), 4)) as list
     from channel_latest c
     where arrays_overlap(array_construct('WhiteIdentitarian', 'QAnon'), c.tags) and reviews_human < 1
)
   , u as (
  select * from file_lists
  union all select *  from random_under_reviewed
  union all select * from popular_under_reviewed
  union all select * from popular_non_reviewed_tag
  union all select * from whide_id_under_reviewed
)
, s as (
  select u.channel_id, u.list
       , channel_title
       , description
       , logo_url
       , reviews_algo
       , reviews_all
       , channel_views
  from u
         left join channel_latest c on c.channel_id=u.channel_id
)
select * from s