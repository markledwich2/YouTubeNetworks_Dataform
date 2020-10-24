with bad_data as (
  select v:VideoId::string as video_id
       , v:Title::string as video_title
       , v:ChannelTitle::string as channel_title

       -- YT returns an upload date that doesn't make sense (usually newer than it was uploaded).
       -- Fall back to added_date when they disagree as it is more reliable (we started collecting this in 2020-09)
       , iff(v:AddedDate::date is not null and abs(datediff(d, v:UploadDate::timestamp_ntz, v:AddedDate::date))>1,
             v:AddedDate::date, v:UploadDate::timestamp_ntz) upload_date
       , v:UploadDate::timestamp_ntz as upload_date_old
       , v:AddedDate::date as added_date
       , datediff(d, upload_date::date, added_date) added_diff
       , iff(v:Updated::timestamp_ntz='0001-01-01'::timestamp_ntz, '2019-11-05'::timestamp_ntz,
             v:Updated::timestamp_ntz) as updated
  from video_stage v
  where updated>'2020-10-01'
    --channel_title = 'The NPC Show'
    and added_date is not null
    --and abs(added_diff)>1
    qualify row_number() over (partition by video_id order by updated desc)=1
  order by channel_title
)
   , summary as (
  select channel_title
       , avg(added_diff) avg_diff
       , count_if(abs(added_diff)>1)/count(*) pct_with_issue
       , count_if(datediff(d, upload_date::date, updated)<=1) updated_is_uploaded
  from bad_data
  group by 1
  order by pct_with_issue desc
)
select *
from summary