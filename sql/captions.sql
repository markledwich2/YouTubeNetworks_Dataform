copy into @yt_testdata/results/search/ from (
    select channel_id
         , video_id
         , video_title
         , description
         , thumb_standard
         , captions
         , upload_date
         , views
    from caption
    where upload_date > '2020-01-01'::timestamp_ntz
      and (video_title like '%corona%' or description like '%corona%' or captions like '%corona%')
      and views > 100000
)
    file_format = (type = parquet ) header=true;

with c as (
    select v:Updated::timestamp_ntz as updated
         , v:VideoId::string as video_id
         , t.value: Text::string as text
         , try_to_time(t.value: Offset::string) as offset_time
    from caption_stage s
       , lateral flatten(input => v:Captions) t
    --${ when(incremental(), ` where updated > (SELECT MAX(updated) FROM ${self()})`) }
)
   , v1 as (
    select c.video_id
         , c.updated
         , array_agg(c.text) within group ( order by offset_time ) as text_array
    from c
    group by video_id, updated
)
   , v2 as (
    select v1.video_id
         , v1.updated
         , array_to_string(v1.text_array, '\n') as captions
         , v.upload_date
         , v.views
         , v.video_title
         , v.channel_title
         , v.channel_id
         , v.description
         , v.pct_ads
         , v.keywords
         , v.duration
         , v.likes
         , v.dislikes
         , v.thumb_standard
    from v1
             left join video_latest v
                       on v1.video_id = v.video_id
)
select *
from v2;

with c as (
    select v:Updated::timestamp_ntz as updated
         , v:VideoId::string as video_id
         , t.value: Text::string as caption
         , timediff(second, '00:00:00'::time, try_to_time(t.value: Offset::string)) as offset_seconds

         --,  'https://youtu.be/' || video_id || '?t=' || timediff(second, '00:00:00'::time, offset_time) as watch_url
    from caption_stage s
       , lateral flatten(input => v:Captions) t
    where video_id in ('2-2-Y3ocGxU','E3URhJx0NSw')
    --${ when(incremental(), ` where updated > (SELECT MAX(updated) FROM ${self()})`) }
)
   , v2 as (
    select c.*
         , cl.ideology
         , cl.media
         , cl.country
         , cl.lr
         , v.upload_date
         , v.views
         , v.video_title
         , v.channel_title
         , v.channel_id
         , v.description
         , v.pct_ads
         , array_to_string(v.keywords, ', ') as keywords
         , v.duration
         , v.likes
         , v.dislikes
         , v.thumb_high
    from c
             left join video_latest v on c.video_id = v.video_id
             left join channel_latest cl on v.channel_id = cl.channel_id
)
select *
from v2;



with captions_carona as (
    select *
    from caption c
    where upload_date >= :upload_from
      and views > :min_views
      and c.caption rlike :corona_regex--'.*\\W(corona(-?virus)?|covid(-?19)?|(SARS-CoV-2)|pandemic)\\W.*'
)
select *
from caption c
where exists(select *
             from captions_carona cc
             where c.video_id = cc.video_id
               and cc.offset_seconds between c.offset_seconds - :leway_seconds and c.offset_seconds + :leway_seconds)
;

select channel_title, count(*) / 3 / 5 as estimated_records
from caption c
where exists(select *
             from captions_carona cc
             where c.video_id = cc.video_id
               and cc.offset_seconds between c.offset_seconds - :leway_seconds and c.offset_seconds + :leway_seconds)
   --or exists(select * from video_title_corona cc where c.video_id = cc.video_id)
group by rollup (channel_title)
order by estimated_records desc