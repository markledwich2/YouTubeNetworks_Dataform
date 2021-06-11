with vids as (
  select e.video_id
       , e.views
       , timediff(seconds,'0'::time,e.duration) secs
       , row_number() over (partition by c.channel_id order by e.views desc nulls last ) rank_in_chan
  from video_extra e
         join channel_latest c on c.channel_id=e.channel_id
  where
    --array_contains('QAnon'::variant,tags)
    e.platform in ('BitChute','Rumble')
    and e.media_url is not null
  order by views desc
  limit :num_videos
)
select count(*) videos
     , sum(secs)/60 mins
     , least(mins,250000)*0.0240 t1
     , iff(mins>250000,least(mins-250000,750000)*0.0150,0) t2
     , iff(mins>5000000,least(mins-1000000,1000000)*0.0102,0) t3
     , t1+t2+t3 total
from vids