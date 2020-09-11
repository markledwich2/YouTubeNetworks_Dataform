with review_lists_raw as (
  select regexp_substr(metadata$filename, '([\\w.]+)\.txt', 1, 1, 'e') as list
       , $1::string as channel_id
  from @public.yt_data/import/review_lists (file_format => tsv)
)
   , review_lists as (
  select r.channel_id, channel_title, description, coalesce(reviews_human, 0) as reviews_human, list
  from review_lists_raw r
         left join channel_latest c on c.channel_id=r.channel_id
)
   , for_review as (
  select *
  from (
         select channel_id, channel_title, description, reviews_human, 'auto' as list
         from channel_latest
         where reviews_human<2
           and meets_sub_criteria
       )
         sample (:limit rows)
)
   , r1 as (
  select *
  from review_lists
  union all
  select *
  from for_review
)
select *
from r1