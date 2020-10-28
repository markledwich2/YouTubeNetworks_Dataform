select channel_title
     , array_to_string(tags, '|') as tags
     , channel_id
     , lr
     , relevance
     , r.value:lr::string as reviewer_lr
     , array_to_string(r.value:tags::array, '|') as reviewer_tags
     , r.value:main_channel_id::string as reviewer_main_channel_id
     , r.value:relevance::double as reviewer_relevance
     , left(r.value: email::string, 3) as reviewer_code
     , r.value:updated::timestamp_ntz updated
     , public_reviewer_notes
     , public_creator_notes
from channel_review
   , lateral flatten(input => reviews) r
order by updated desc