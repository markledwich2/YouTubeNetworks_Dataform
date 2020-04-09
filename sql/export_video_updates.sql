copy into @yt_results/export/2020-06-09/video_updates.csv.gz from (
    select video_id
         , video_title
         , channel_id
         , channel_title
         , upload_date
         , min(updated) earliest_update
         , max(updated) latest_update
         , count(*) number_of_updates
         , min(views) min_views
         , max(views) max_views
        , datediff(day, upload_date, latest_update) as age_at_latest_update
    from video
    group by 1, 2, 3, 4, 5
)
file_format = (type ='csv' field_optionally_enclosed_by = '"' compression = 'gzip') header=true single=true max_file_size=268435456, overwrite=true
