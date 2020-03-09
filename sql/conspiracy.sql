-- load data from https://github.com/youtube-dataset/conspiracy for study https://farid.berkeley.edu/downloads/publications/arxiv20.pdf
-- transform into a flat format easy to view and compare with our dataset

create or replace file format json type = 'json';

with raw as (
    select parse_json($1) v from @yt/external/conspiracy/training_set.json (file_format => json)
),
title as (
    select c.key as video_id, c.value as title
    from raw
       , lateral flatten(input => v:title, outer => true) c
),
label as (
    select c.key as video_id, c.value::int as conspiracy
    from raw
       , lateral flatten(input => v:label, outer => true) c
    )
select t.video_id, t.title, l.conspiracy, v.channel_title, c.ideology
from title t left join label l on t.video_id = l.video_id
left join video_latest v on v.video_id = t.video_id
left join channel_latest c on v.channel_id = c.channel_id
;

with
raw as (
    select parse_json($1) v from @yt/external/conspiracy/conspiracy_results.json (file_format => json)
),
title as (
    select c.key as video_id, c.value as title
    from raw
       , lateral flatten(input => v:title, outer => true) c
),
rec as (
    select c.key as video_id, c.value::int as rec_count
    from raw
       , lateral flatten(input => v:reco_count, outer => true) c
    ),
likelyhood as (
        select c.key as video_id, c.value::double as conspiracy_likelihood
    from raw
       , lateral flatten(input => v:conspiracy_likelihood, outer => true) c
    ),
date as (
        select c.key as video_id, to_date(c.value::string, 'dd-MM-yyyy') as date
    from raw
       , lateral flatten(input => v:query_date, outer => true) c
    )

select t.video_id, t.title, r.rec_count, l.conspiracy_likelihood, d.date, c.channel_title, c.ideology
from title t
left join rec r on t.video_id = r.video_id
left join likelyhood l on t.video_id = l.video_id
left join date d on t.video_id = d.video_id
left join video_latest v on v.video_id = t.video_id
left join channel_latest c on v.channel_id = c.channel_id
;


with
raw as (
    select parse_json($1) v from @yt/external/conspiracy/seed_channels.json (file_format => json)
),
channel as (
    select c.key as channel_id, c.value as title
    from raw
       , lateral flatten(input => v:title, outer => true) c
)
select c.*, l.ideology, l.channel_views
from channel c
left join channel_latest l on c.channel_id = l.channel_id;