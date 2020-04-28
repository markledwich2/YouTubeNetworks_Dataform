copy into @YT_RESULTS/user_scrape/video_seeds.csv.gz
    from (
        -- videos with view rank within an  channel
        with v1 as (
            select c.channel_title
                 , c.channel_id
                 , c.ideology
                 , video_id
                 , video_title
                 , views
                 , rank() over (partition by c.channel_id order by views desc) as video_rank_in_channel
                 , sum(views)
                       over (partition by c.channel_id order by views desc rows unbounded preceding) as running_views
            from video_latest v
                     left join channel_latest c on v.channel_id = c.channel_id
            where c.channel_id is not null
              and views > 0
              and upload_date > '2019-01-01'
        )
           -- v1 + last running_views with in ideology
           , v2 as (
            select *
                 , sum(views) over (partition by channel_id) as channel_views_total
                 , coalesce(lag(running_views) over (partition by channel_id order by views desc),
                            0) as last_running_views
            from v1
        )

           -- 10 random numbers between 0-1 for each channel
           , c_rand as (
            select c.channel_id, uniform(0, 100000000, random()) / 100000000 as rand, g.gen_no
            from (select distinct channel_id from channel_latest) c
            cross join (select seq8() as gen_no from table (generator(rowcount => 10))) g
        )

           -- choose channel videos weighted by their views
           , s as (
            select v2.*
                 , r.rand * v2.channel_views_total as rand_view
                 , rand_view
                       > last_running_views and rand_view
                       < running_views as chosen
                 , gen_no
            from v2
                     left join c_rand r on v2.channel_id = r.channel_id
            where chosen
        )
           , s2 as (
            -- top 50 distinct videos spread across channels in each ideology group
            select *, row_number() over (partition by ideology order by gen_no, channel_views_total desc) as ideology_rank
            from (
                     -- distinct rows because some channels have the same video selected at random many times
                     select ideology
                          , channel_id
                          , channel_title
                          , video_id
                          , video_title
                          , any_value(views) as views
                          , any_value(channel_views_total) as channel_views_total
                          , any_value(video_rank_in_channel) as video_rank_in_channel
                          , min(gen_no) as gen_no
                     from s
                     group by 1, 2, 3, 4, 5
                 )
        )
        select ideology, channel_id, channel_title, video_id, video_title, ideology_rank
        from s2
        where ideology_rank < 50
        order by ideology, ideology_rank
    )
    file_format = (type ='csv' field_optionally_enclosed_by = '"' compression = 'gzip') header=true single=true max_file_size=268435456, overwrite=true;