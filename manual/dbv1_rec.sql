
create table if not exists dbv1_rec_stage
(
    v variant
);

copy into dbv1_rec_stage
    from @dbv1/RecommendedVideos/
    file_format = (type = json);


-- dbv1_rec_flat: there are two levels of nested recommends. flatten it out
create or replace table dbv1_rec as
with flatten1 as (
    select fv.video_id                    as from_video_id,
           fv.video_title                 as from_video_title,
           fv.channel_id                  as from_channel_id,
           fv.channel_title               as from_channel_title,
           r.value:updated::timestamp_ntz as updated,
           r.value:recommended::array     as recommended
    from dbv1_rec_stage rv1
             left join video_latest fv on fv.video_id = rv1.v:videoId::string,
         lateral flatten(input => v:recommended) r
)
   , flatten2 as (
    select r1.from_channel_id,
           r1.from_channel_title,
           r1.from_video_id,
           r1.from_video_title,
           r2.value:channelId::string    as to_channel_id,
           r2.value:channelTitle::string as to_channel_title,
           r2.value:rank::int            as rank,
           r2.value:videoId::string      as to_video_id,
           r2.value:videoTitle::string   as to_video_title,
           r1.updated,
           date_trunc('day', updated)    as updated_day
    from flatten1 r1,
         lateral flatten(input => r1.recommended) r2
)
select *
from flatten2
where rank <= 10;