select channel_title
     , tags
     , channel_id
     , lr
     , relevance
     , r.value:lr::string as reviewer_lr
     , r.value:tags::array as reviewer_tags
     , r.value:main_channel_id::string as reviewer_main_channel_id
    , r.value:relevance::double as reviewer_relevance
, left(r.value:email::string, 3) as reviewer_code
from channel_review
   , lateral flatten(input => reviews) r