select
  to_char(p.meeting_date, 'YYYY-MM-DD') as meeting_date,
  case when date_part(dow, p.meeting_date) = 0
    then 7::smallint
    else date_part(dow, p.meeting_date)::smallint
    end as day_of_week,
  p.country_code,
  p.course_name as track,
  p.race_number,
  to_char(r.scheduled_time_of_race_local, 'YYYY-MM-DD hh24:mi:ss') as race_time,
  r.race_title,
  p.horse_name,
  p.betfair_win_s_p as bsp,
  p.betfair_place_s_p as bpsp,
  s.sp,
  s.win_prob,
  s.place_prob,
  p.position_official as position,
  r.number_of_runners as runners,
  r.race_surface_name as surface,
  r.race_type,
  r.going,
  r.distance,
  r.race_class,
  r.race_code,
  case when r.eligibility_age_max like '^[2-9]*$'
    then r.eligibility_age_min || '-' || r.eligibility_age_max
    else r.eligibility_age_min || r.eligibility_age_max
    end as age,
  nullif(r.eligibility_sex_limit, '') as sex_limit,
  r.prize_fund::int,
  r.tv,
  s.bf_event_id as market_id,
  --case when r.bf_market_id in ('', '0')
    --then null
    --else r.bf_market_id::bigint
    --end as market_id,
  case when e.bf_selection_id in ('', '0')
    then null
    else e.bf_selection_id::bigint
    end as selection_id
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
left join omni_betevent.vw_racing_selection_summary s
  on s.start_time_uki = r.scheduled_time_of_race_local
  and s.bf_selection_id = e.bf_selection_id
where p.meeting_date = ?
  and p.country_code in ('GBR', 'IRE')
  and p.betfair_win_s_p > 0
order by p.meeting_date, p.course_name, p.race_number, position
