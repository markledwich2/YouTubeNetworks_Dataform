create table if not exists dbv1_video_stage
(
    v variant
);
copy into dbv1_video_stage
    from @dbv1/Videos/
    file_format = (type = json);

create or replace table dbv1_video as
select v:latest:videoId::string            as video_id,
       v:latest:videoTitle::string         as video_title,
       v:latest:channelId::string          as channel_id,
       v:latest:channelTitle::string       as channel_title,
       v:latest:publishedAt::timestamp_ntz as upload_date,
       h.value: views::int                 as views,
       h.value:likes::int                  as likes,
       h.value:dislikes::int               as dislikes,
       v:latest:description::string        as description,
       null::time                          as duration,
       h.value:updated::timestamp_ntz      as updated
from dbv1_video_stage v,
     lateral flatten(input => array_cat(array_construct(v:latest:stats), v: history)) h;