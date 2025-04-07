with p1_dates as (
select
playerId, team, concat(date(start_date), '—', date(end_date)) as term_date, start_date
from skaters
where team = 'Edmonton Oilers'
group by playerId, start_date
order by start_date
),
p2_dates as (
select
playerId, team, concat(date(start_date), '—', date(end_date)) as term_date, start_date 
from skaters
where team = 'Calgary Flames'
group by playerId, start_date
order by start_date
),
p1_dates_agg as (
select playerId, team, string_agg(p1_dates.term_date, ', ') as dates, start_date
from p1_dates
group by playerId
),
p2_dates_agg as (
select playerId, team, string_agg(p2_dates.term_date, ', ') as dates, start_date
from p2_dates
group by playerId
)
select playerId, playerName, p1_dates_agg.team, p1_dates_agg.dates, p2_dates_agg.team, p2_dates_agg.dates
from p1_dates_agg join p2_dates_agg using (playerId) join links using (playerId)
order by p1_dates_agg.start_date;