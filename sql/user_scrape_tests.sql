
-- get a list of videos to test
-- gets the top 5 videos in the last 7 days for each ideology (distinct channels)
copy into @YT_RESULTS/user_scrape/video_tests.csv.gz
  from (
    -- top 1 video in the last 7 days from each channel
    with top_each_channel as (
      select video_id
           , video_title
           , c.channel_id
           , c.channel_title
           , views
           , c.ideology

      from video_latest v
             inner join channel_latest c on v.channel_id = c.channel_id
      where upload_date > dateadd(day, -7, (select max(upload_date) from video_latest))
        qualify rank() over (partition by c.channel_id order by views desc) = 1
    )

    -- top 5 within each ideology
    select *
    from top_each_channel
      qualify rank() over (partition by ideology order by views desc) <= 5
    order by ideology, views desc
  )
  file_format = (type ='csv' field_optionally_enclosed_by = '"' compression = 'gzip') header=true single=true max_file_size=268435456, overwrite=true;