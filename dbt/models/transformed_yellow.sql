-- joins green with emissions table where vehicle type matches, extracts calculations for co2 and hour/day/week/month into 5 new columns
-- includes error handling for dividing by 0
select
    g.*,
    (g.trip_distance * e.co2_grams_per_mile / 1000.0) as trip_co2_kgs,
    (g.trip_distance / nullif(g.trip_duration_s / 3600.0, 0)) as avg_mph,
    extract(hour from g.lpep_pickup_datetime) as hour_of_day,
    extract(dow from g.lpep_pickup_datetime) as day_of_week,
    extract(week from g.lpep_pickup_datetime) as week_of_year,
    extract(month from g.lpep_pickup_datetime) as month_of_year
from green_clean g
join emissions e
  on e.vehicle_type = 'green_taxi'
