
copy into @yt_testdata/user_scrape/input_data/videos_to_watch.csv.gz
    from (
        -- videos to scrape
        with v1 as (
            select c.channel_title
                 , c.channel_id
                 , c.ideology
                 , video_id
                 , video_title
                 , views
                 , rank() over (partition by c.ideology order by views desc) as video_rank
                 , sum(views)
                       over (partition by c.ideology order by views desc rows unbounded preceding) as running_views
            from video_latest v
                     left join channel_latest c on v.channel_id = c.channel_id
            where ideology is not null
              and views > 0
              and upload_date > '2019-11-11'
        )
           , v2 as (
            select *
                 , sum(views) over (partition by ideology) as channel_views_total
                 , coalesce(lag(running_views) over (partition by ideology order by views desc),
                            0) as last_running_views
            from v1
        )
           , c_rand as (
            select ideology, uniform(0, 100000000, random()) / 100000000 as rand
            from (select distinct ideology from channel_latest)
                     cross join table (generator(rowcount => 200))
        )
           , s as (
            select v2.*
                 , r.rand * v2.channel_views_total as rand_view
                 , rand_view
                       > last_running_views and rand_view
                       < running_views as chosen
            from v2
                     left join c_rand r on v2.ideology = r.ideology
            where chosen
        )
           , s2 as (
            select ideology, channel_id, channel_title, video_id, video_title, views
            from s
            order by ideology, video_rank
        )
        select *
        from s2
    )
    file_format = (type ='csv' field_optionally_enclosed_by = '"' compression = 'gzip') header=true single=true max_file_size=268435456, overwrite=true