-- src/pipeline/gold.sql — consumer-facing objects, written to the gold schema by fully qualified name

CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_otp_carrier_month
CLUSTER BY (period)
COMMENT 'Monthly on-time and cancellation rates per reporting carrier'
AS SELECT
  f.period,
  f.carrier_code,
  c.carrier_name,
  count(*)                                                                      AS scheduled_flights,
  count_if(f.is_completed)                                                      AS operated_flights,
  count_if(f.is_completed AND f.arr_del15 = 0)                                  AS on_time_flights,
  count_if(f.is_completed AND f.arr_del15 = 0) / nullif(count_if(f.is_completed), 0) AS on_time_rate,
  count_if(f.cancelled) / count(*)                                              AS cancellation_rate
FROM silver_flights f
LEFT JOIN dim_carrier c ON f.carrier_code = c.carrier_code
GROUP BY ALL;

-- gold_delay_causes   (year x carrier): minutes per cause and per 100 operated flights
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_delay_causes
COMMENT 'Delay minutes by cause per carrier and year, normalized per 100 operated flights'
AS
SELECT
  year(f.flight_date) AS year,
  f.carrier_code,
  c.carrier_name,

  count_if(f.is_completed) AS operated_flights,

  sum(f.carrier_delay) AS carrier_delay_min,
  sum(f.weather_delay) AS weather_delay_min,
  sum(f.nas_delay) AS nas_delay_min,
  sum(f.security_delay) AS security_delay_min,
  sum(f.late_aircraft_delay) AS late_aircraft_delay_min,

  100.0 * sum(f.carrier_delay)
    / nullif(count_if(f.is_completed), 0) AS carrier_delay_per_100_operated,

  100.0 * sum(f.weather_delay)
    / nullif(count_if(f.is_completed), 0) AS weather_delay_per_100_operated,

  100.0 * sum(f.nas_delay)
    / nullif(count_if(f.is_completed), 0) AS nas_delay_per_100_operated,

  100.0 * sum(f.security_delay)
    / nullif(count_if(f.is_completed), 0) AS security_delay_per_100_operated,

  100.0 * sum(f.late_aircraft_delay)
    / nullif(count_if(f.is_completed), 0) AS late_aircraft_delay_per_100_operated

FROM silver_flights f
LEFT JOIN dim_carrier c
  ON f.carrier_code = c.carrier_code
GROUP BY ALL;

-- gold_airport_hour   (year x origin x dep_hour): departures, avg dep delay, cancellation rate
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_airport_hour
COMMENT 'Departure performance by origin airport, year and scheduled departure hour'
AS
SELECT
  year(f.flight_date) AS year,
  f.origin,
  a.airport_name,
  f.dep_hour,

  count(*) AS scheduled_departures,
  avg(f.dep_delay_min) AS avg_dep_delay_min,
  count_if(f.cancelled) / count(*) AS cancellation_rate

FROM silver_flights f
LEFT JOIN dim_airport a
  ON f.origin = a.airport_code
GROUP BY ALL;

-- gold_weather_impact (origin x flight_date): flights, cancellations, avg dep delay + prcp, snow via ref_airport_station
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_weather_impact
CLUSTER BY (flight_date, origin)
COMMENT 'Daily flight performance joined to NOAA weather by airport and date'
AS
WITH flight_daily AS (
  SELECT
    flight_date,
    origin,

    count(*) AS flights,
    count_if(cancelled) AS cancelled_flights,
    count_if(cancelled) / count(*) AS cancellation_rate,
    avg(dep_delay_min) AS avg_dep_delay_min

  FROM silver_flights
  GROUP BY flight_date, origin
),

station_map AS (
  SELECT
    airport_code,
    regexp_replace(station_id, '^GHCND:', '') AS station_id
  FROM ${a2.lakehouse}.ref_airport_station
)

SELECT
  f.origin,
  f.flight_date,
  f.flights,
  f.cancelled_flights,
  f.cancellation_rate,
  f.avg_dep_delay_min,

  w.prcp,
  w.snow,
  w.tmax,
  w.tmin

FROM flight_daily f
LEFT JOIN station_map m
  ON f.origin = m.airport_code
LEFT JOIN silver_weather_daily w
  ON m.station_id = w.station_id
 AND f.flight_date = w.obs_date;

-- The governed object: row filter and mask are part of the definition, so every refresh keeps them.
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_aircraft_rotation (
  flight_date          DATE,
  carrier_code         STRING,
  tail_number          STRING MASK ${a2.ops}.mask_text,
  legs                 BIGINT,
  first_sched_dep      TIMESTAMP,
  last_sched_dep       TIMESTAMP,
  airports_visited     BIGINT,
  delayed_legs         BIGINT,
  total_arr_delay_min  DOUBLE
)
WITH ROW FILTER ${a2.ops}.rf_scope ON (carrier_code)
CLUSTER BY (flight_date, carrier_code)
COMMENT 'Daily rotation per aircraft. Rows filtered by carrier entitlement, tail numbers masked.'
AS SELECT
  flight_date,
  carrier_code,
  tail_number,
  count(*)                    AS legs,
  min(sched_dep_ts)           AS first_sched_dep,
  max(sched_dep_ts)           AS last_sched_dep,
  count(DISTINCT origin)      AS airports_visited,
  count_if(arr_del15 = 1)     AS delayed_legs,
  sum(arr_delay_min)          AS total_arr_delay_min
FROM silver_flights
WHERE tail_number IS NOT NULL AND NOT cancelled
GROUP BY ALL;

-- gold_disruptions    (flight): cancelled or arr_delay_min >= 180, with carrier_code, tail_number and flight_number
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_disruptions
CLUSTER BY (flight_date)
COMMENT 'Cancelled flights or flights arriving at least 180 minutes late'
AS
SELECT
  flight_date,
  period,
  carrier_code,
  flight_number,
  tail_number,
  origin,
  dest,
  sched_dep_ts,
  cancelled,
  cancellation_code,
  arr_delay_min,
  dep_delay_min,
  route
FROM silver_flights
WHERE cancelled
   OR arr_delay_min >= 180;

-- gold_tail_delays    (period x carrier x tail_number): flights, delayed share, avg arrival delay

CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_tail_delays
CLUSTER BY (period, carrier_code)
COMMENT 'Monthly delayed-flight share and average arrival delay by aircraft'
AS
SELECT
  period,
  carrier_code,
  tail_number,

  count(*) AS flights,

  count_if(arr_del15 = 1) AS delayed_flights,

  count_if(arr_del15 = 1)
    / nullif(count(*), 0) AS delayed_share,

  avg(arr_delay_min) AS avg_arr_delay_min

FROM silver_flights
WHERE tail_number IS NOT NULL
GROUP BY ALL;