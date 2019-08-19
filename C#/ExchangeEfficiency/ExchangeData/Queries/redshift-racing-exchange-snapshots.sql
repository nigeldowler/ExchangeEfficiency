-- Exchange snapshot data --

-- Instructions:
-- Feed a list of "seconds before the off" into the temp table insert command
-- Declare a market_id in the "ids" with block
-- This works for UK&I horse racing but can be amended for other sports etc.
-- in the where clause of the "unmatched" block

drop table if exists temp_seconds_before_off;

create temporary table temp_seconds_before_off (
  seconds_before_off bigint encode ZSTD
);

insert into temp_seconds_before_off values
{0}
;
--END OF INSERT--

with ids as (
  select
    ? as market_id
),

unmatched as (
  select
    e.market_id,
    e.market_off_datetime,
    dateadd(second, -t.seconds_before_off, e.market_off_datetime) as snapshot_time,
    t.seconds_before_off,
    e.selection_id,
    e.selection_name,
    e.bet_type,
    e.placed_bet_price,
    round(sum(e.bet_amount_gbp), 2) as total_bet_amount_gbp,
    rank() over (
      partition by e.market_id, e.selection_id, t.seconds_before_off, e.bet_type
      order by
        case when e.bet_type = 'L' then e.placed_bet_price end desc,
        case when e.bet_type = 'B' then e.placed_bet_price end asc
    ) as rnk

  from
    omni_exchange.bf_vw_exchange_bet_all e,
    temp_seconds_before_off t

  where
    e.market_id = (select market_id from ids)
    and e.sport_id = 7
    and e.market_country_id in (1, 35)
    and e.placed_in_play_yn = 'N'
    and e.bsp_bet_type = ''
    and e.placed_datetime <= dateadd(second, -t.seconds_before_off, e.market_off_datetime)
    and (e.match_placed_datetime is null or e.placed_datetime <= e.match_placed_datetime)
    and (e.settled_datetime is null or e.settled_datetime > dateadd(second, -t.seconds_before_off, e.market_off_datetime))
    and (e.taken_datetime is null or e.taken_datetime > dateadd(second, -t.seconds_before_off, e.market_off_datetime))
    and (e.cancelled_datetime is null or e.cancelled_datetime > dateadd(second, -t.seconds_before_off, e.market_off_datetime))

  group by market_id, market_off_datetime, snapshot_time, seconds_before_off, selection_id, selection_name, bet_type, placed_bet_price
)

select
  market_id,
  to_char(market_off_datetime, 'YYYY-MM-DD hh24:mi:ss') as market_off_datetime,
  to_char(snapshot_time, 'YYYY-MM-DD hh24:mi:ss') as snapshot_time,
  seconds_before_off,
  selection_id,
  selection_name,
  max(case when bet_type = 'L' and rnk = 3 then placed_bet_price end) as back_3,
  max(case when bet_type = 'L' and rnk = 3 then total_bet_amount_gbp end) as back_3_vol,
  max(case when bet_type = 'L' and rnk = 2 then placed_bet_price end) as back_2,
  max(case when bet_type = 'L' and rnk = 2 then total_bet_amount_gbp end) as back_2_vol,
  max(case when bet_type = 'L' and rnk = 1 then placed_bet_price end) as back,
  max(case when bet_type = 'L' and rnk = 1 then total_bet_amount_gbp end) as back_vol,
  max(case when bet_type = 'B' and rnk = 1 then placed_bet_price end) as lay,
  max(case when bet_type = 'B' and rnk = 1 then total_bet_amount_gbp end) as lay_vol,
  max(case when bet_type = 'B' and rnk = 2 then placed_bet_price end) as lay_2,
  max(case when bet_type = 'B' and rnk = 2 then total_bet_amount_gbp end) as lay_2_vol,
  max(case when bet_type = 'B' and rnk = 3 then placed_bet_price end) as lay_3,
  max(case when bet_type = 'B' and rnk = 3 then total_bet_amount_gbp end) as lay_3_vol
from unmatched
where rnk <= 3
group by market_id, market_off_datetime, snapshot_time, seconds_before_off, selection_id, selection_name
order by market_id, seconds_before_off desc, selection_id
