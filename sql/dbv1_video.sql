-- once of load of old video data
copy into dbv1_video_stage
    from @yt_data/db/Videos/
    file_format = (type = json);

select * from dbv1_video_stage;
