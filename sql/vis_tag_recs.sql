with tags as (
  select distinct t.value::string as tag
  from channel_accepted
     , table (flatten(tags)) t
  where tag not in ('AntiWhiteness','Educational','MissingLinkMedia','Politician','Provocateur','Revolutionary','StateFunded')
)
   , tags_combo as (
  select from_tags.tag as from_tag, to_tags.tag as to_tag
  from tags as from_tags
         cross join tags as to_tags
)
   , channel_combo as (
  select t.*, cf.channel_id as from_channel_id, ct.channel_id as to_channel_id, ct.channel_title as to_channel_title
  from tags_combo t
         left join channel_accepted cf on array_contains(t.from_tag::variant, cf.tags)
         left join channel_accepted ct on array_contains(t.to_tag::variant, ct.tags)
     where from_channel_id <> to_channel_id
)
   , tag_recs as (
  select c.from_tag
       , c.to_tag
       , sum(relevant_impressions/datediff(day, :from::date, least(last_day(:to::date, month), current_date()))) as relevant_impressions_daily
       , count(distinct r.to_channel_id) as to_channels_total
  from channel_recs_monthly r
         left join channel_combo c on r.from_channel_id=c.from_channel_id and r.to_channel_id=c.to_channel_id
  where rec_month between :from::date and last_day(:to::date, month)
    and c.from_tag is not null
    and c.to_tag is not null
  group by c.from_tag, c.to_tag
)
select *
from tag_recs
order by from_tag, relevant_impressions_daily desc;

