select
  case when r.bf_market_id in ('', '0')
    then null
    else r.bf_market_id::bigint
    end as market_id,
  to_char(r.meeting_date, 'YYYY-MM-DD') as meeting_date,
  case
    when r.country_code = 'GBR' then 'GB'
    when r.country_code = 'IRE' then 'IE'
    else null
    end as country_code,
  r.course_name as track,
  r.race_number,
  to_char(r.scheduled_time_of_race_gmt, 'YYYY-MM-DD hh24:mi:ss') as race_time,
  r.race_title,
  r.number_of_runners as runners,
  case when e.bf_selection_id in ('', '0')
    then null
    else e.bf_selection_id::bigint
    end as selection_id,
  p.horse_name as selection_name,
  p.horse_age,
  p.horse_gender as horse_sex,
  p.weight_carried,
  nullif(p.handicap_mark, 0) as handicap_mark,
  nullif(p.draw, 0) as draw,
  p.jockey_name as jockey,
  p.trainer_name as trainer,
  p.owner_full_name as "owner",
  p.betfair_win_s_p as bsp,
  p.isp_decimal as sp,
  rank() over (partition by r.bf_market_id, p.isp_decimal > 1 order by p.isp_decimal) as fav_rank, --might need to check for late NRs
  p.position_official as position,
  p.ip_min as inplay_min,
  p.ip_max as inplay_max,
  r.race_surface_name as surface,
  r.race_type,
  r.going,
  r.distance,
  coalesce(nullif(r.race_class, '')::smallint, trd.class_id) as race_class,
  r.race_code,
  case
    when trd.handicap = true
      or upper(r.race_title) like '%HANDICAP%'
      or upper(r.race_title) like '%HCAP%'
      or upper(r.race_title) like '%H''CAP%'
      or upper(r.race_title) like '%NURSERY%'
      then 1
      else 0
    end as handicap,
  case when r.eligibility_age_max like '^[2-9]*$'
    then r.eligibility_age_min || '-' || r.eligibility_age_max
    else r.eligibility_age_min || r.eligibility_age_max
    end as age,
  nullif(r.eligibility_sex_limit, '') as sex_limit,
  r.prize_fund::int,
  r.tv
 
from omni_betevent.timeform_vw_performance p

right join omni_betevent.timeform_vw_race r
  on r.meeting_date = p.meeting_date
  and r.course_id = p.course_id
  and r.race_number = p.race_number

left join omni_betevent.timeform_vw_entry e
  on e.meeting_date = p.meeting_date
  and e.course_id = p.course_id
  and e.race_number = p.race_number
  and e.horse_code = p.horse_code

left join omni_betevent.vw_ramp_race rmp
  on r.scheduled_time_of_race_gmt = rmp.start_datetime
  and (
    (r.country_code = 'GBR' and rmp.country_id = 1)
    or
    (r.country_code = 'IRE' and rmp.country_id = 0)
  )

left join omni_betevent.trading_vw_races trd
  on trd.event_id = rmp.ramp_id

where r.country_code in ('GBR', 'IRE')
  and r.results_status = 'Weighed In'
  and r.meeting_date >= ?
  and r.meeting_date < ?

order by r.meeting_date, r.course_name, r.race_number, p.position_official
