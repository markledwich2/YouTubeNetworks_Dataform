select * from dbv1_rec_stage;

-- once off load of old data
copy into dbv1_rec_stage
    from @yt_data/db/RecommendedVideos/
    file_format = (type = json);
