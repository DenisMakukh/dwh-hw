-- \copy src.flights(year, quarter, month, flight_date, op_unique_carrier, tail_num, op_carrier_flight_num, origin_airport_id, dest_airport_id, crs_dep_time, dep_time, dep_delay_new, cancelled, cancellation_code, air_time, distance, weather_delay) FROM '/Users/denvads/PycharmProjects/data-science-hw/kdz/2022_jan_to_apr.csv' WITH DELIMITER ',' CSV HEADER;
-- \copy src.airports(id, ident, type, name, latitude_deg, longitude_deg, elevation_ft, continent, iso_country, iso_region, municipality, scheduled_service, gps_code, iata_code, local_code, home_link, wikipedia_link, keywords) FROM '/Users/denvads/PycharmProjects/data-science-hw/kdz/data_src/airports.csv' WITH DELIMITER ',' CSV HEADER;
-- \copy src.weather(local_time_melbourne_airport, t, p0, p, u, dd, ff, ff10, ww, ww_alternate, c, vv, td) FROM '/Users/denvads/PycharmProjects/data-science-hw/kdz/data_src/weather_kmlb_cleared.csv' WITH DELIMITER ',' CSV HEADER;

drop table if exists etl.flights_new;
create table etl.flights_new AS
select MIN(loaded_ts) AS ts1,
       MAX(loaded_ts) AS ts2
from src.flights
where loaded_ts >= COALESCE((select MAX(loaded_ts) from etl.flights),
                            '1970-01-01')
  and origin_airport_id = 'MLB';

DROP TABLE IF EXISTS etl.flights_temp;

CREATE TABLE etl.flights_temp AS
SELECT DISTINCT ON
(
    flight_date,
    crs_dep_time,
    flight_number,
    origin,
    dest
)
    CAST
(
    flights
    .
    "year" AS
    INT
),
    CAST
(
    flights
    .
    "quarter" AS
    INT
),
    CAST
(
    flights
    .
    "month" AS
    INT
),
    TO_DATE
(
    flight_date,
    'MM/DD/YYYY HH:MI:SS AM'
) AS flight_date,
    CAST
(
    TO_CHAR(
    TO_TIMESTAMP(
    LPAD(
    CAST (
    ROUND(
    CAST (
    NULLIF
(
    dep_time,
    ''
) AS FLOAT)) AS TEXT), 4, '0'),
    'HH24MI'
    ),
    'HH24:MI'
    ) AS TIME) AS dep_time,
    CAST
(
    TO_CHAR(
    TO_TIMESTAMP(
    LPAD(
    CAST (
    ROUND(
    CAST (
    NULLIF
(
    crs_dep_time,
    ''
) AS FLOAT)) AS TEXT), 4, '0'),
    'HH24MI'
    ),
    'HH24:MI'
    ) AS TIME) AS crs_dep_time,
    CAST
(
    air_time AS
    FLOAT
),
    CAST
(
    dep_delay_new AS
    FLOAT
) AS dep_delay_minutes,
    CAST
(
    SUBSTRING
(
    cancelled,
    1,
    1
) AS INT) AS cancelled,
    cancellation_code,
    CAST
(
    weather_delay AS
    FLOAT
),
    op_unique_carrier AS reporting_airline,
    tail_num AS tail_number,
    CAST
(
    op_carrier_flight_num AS
    INT
) AS flight_number,
    CAST
(
    distance AS
    FLOAT
),
    origin_airport_id AS origin,
    dest_airport_id AS dest,
    NOW
(
) AS loaded_ts
    FROM src.flights,
    etl.flights_new
    WHERE loaded_ts >= ts1
    AND loaded_ts <= ts2
    AND origin_airport_id = 'MLB';


INSERT INTO stg.flights ("year", "quarter", "month", flight_date,
                         dep_time, crs_dep_time, air_time, dep_delay_minutes,
                         cancelled, cancellation_code, weather_delay,
                         reporting_airline, tail_number, flight_number, distance,
                         origin, dest, loaded_ts)
select "year",
       "quarter",
       "month",
       flight_date,
       dep_time,
       crs_dep_time,
       air_time,
       dep_delay_minutes,
       cancelled,
       cancellation_code,
       weather_delay,
       reporting_airline,
       tail_number,
       flight_number,
       distance,
       origin,
       dest,
       loaded_ts
from etl.flights_temp
where origin = 'MLB' on conflict (flight_date, crs_dep_time, flight_number, origin, dest) do
update
    set "year" = EXCLUDED."year",
    "quarter" = EXCLUDED."quarter",
    "month" = EXCLUDED."month",
    dep_time = EXCLUDED.dep_time,
    air_time = EXCLUDED.air_time,
    dep_delay_minutes = EXCLUDED.dep_delay_minutes,
    cancelled = EXCLUDED.cancelled,
    cancellation_code = EXCLUDED.cancellation_code,
    weather_delay = EXCLUDED.weather_delay,
    reporting_airline = EXCLUDED.reporting_airline,
    tail_number = EXCLUDED.tail_number,
    distance = EXCLUDED.distance,
    loaded_ts = EXCLUDED.loaded_ts;


ALTER TABLE src.weather
    ADD COLUMN loaded_ts TIMESTAMP DEFAULT NOW();
UPDATE src.weather
SET loaded_ts = NOW()
WHERE loaded_ts IS NULL;

ALTER TABLE stg.weather
    ADD COLUMN loaded_ts TIMESTAMP DEFAULT NOW();

ALTER TABLE src.weather
    ADD COLUMN icao_code varchar(10) NOT NULL default 'KMLB';

CREATE TABLE IF NOT EXISTS etl.weather
(
    loaded_ts
    TIMESTAMP
    NOT
    NULL
    PRIMARY
    KEY
);

DROP TABLE IF EXISTS etl.weather_new;

CREATE TABLE etl.weather_new AS
SELECT MIN(loaded_ts) AS ts1,
       MAX(loaded_ts) AS ts2
FROM src.weather
WHERE loaded_ts >= COALESCE((SELECT MAX(loaded_ts)
                             FROM etl.weather), '1970-01-01');

DROP TABLE IF EXISTS etl.weather_temp;



CREATE TABLE etl.weather_temp AS
SELECT DISTINCT ON
(
    icao_code,
    local_datetime
) icao_code,
    local_time_melbourne_airport as local_datetime,
    cast
(
    t as
    numeric
(
    3,
    1
)) as t_air_temperature,
    cast
(
    p0 as
    numeric
(
    4,
    1
)) as p0_sea_lvl,
    cast
(
    p as
    numeric
(
    4,
    1
)) as p_station_lvl,
    cast
(
    SUBSTRING
(
    u,
    1,
    1
) as INT) as u_humidity,
    dd as dd_wind_direction,
    cast
(
    SUBSTRING
(
    ff,
    1,
    1
) as INT) as ff_wind_speed,
    cast
(
    SUBSTRING
(
    ff10,
    1,
    1
) as INT) as ff10_max_gust_value,
    ww as ww_present,
    ww_alternate as ww_recent,
    c as c_total_clouds,
    cast
(
    vv as
    numeric
(
    3,
    1
)) as vv_horizontal_visibility,
    cast
(
    td as
    numeric
(
    3,
    1
)) as td_temperature_dewpoint,
    loaded_ts
    FROM src.weather,
    etl.weather_new
    WHERE loaded_ts >= ts1
    AND loaded_ts <= ts2;

INSERT INTO stg.weather (icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity,
                         dd_wind_direction, ff_wind_speed, ff10_max_gust_value, ww_present, ww_recent,
                         c_total_clouds,
                         vv_horizontal_visibility, td_temperature_dewpoint, loaded_ts)
SELECT icao_code,
       local_datetime,
       COALESCE(t_air_temperature, 0),
       COALESCE(p0_sea_lvl, 0),
       COALESCE(p_station_lvl, 762.6)        AS p_station_lvl,
       COALESCE(u_humidity, 0),
       dd_wind_direction,
       ff_wind_speed,
       ff10_max_gust_value,
       ww_present,
       ww_recent,
       COALESCE(c_total_clouds, 'No clouds') AS c_total_clouds,
       COALESCE(vv_horizontal_visibility, 0),
       COALESCE(td_temperature_dewpoint, 0),
       loaded_ts
FROM etl.weather_temp ON CONFLICT (icao_code, local_datetime) DO
UPDATE
    SET t_air_temperature = EXCLUDED.t_air_temperature,
    p0_sea_lvl = EXCLUDED.p0_sea_lvl,
    p_station_lvl = COALESCE (EXCLUDED.p_station_lvl, 762.6),
    u_humidity = EXCLUDED.u_humidity,
    dd_wind_direction = EXCLUDED.dd_wind_direction,
    ff_wind_speed = EXCLUDED.ff_wind_speed,
    ff10_max_gust_value = EXCLUDED.ff10_max_gust_value,
    ww_present = EXCLUDED.ww_present,
    ww_recent = EXCLUDED.ww_recent,
    c_total_clouds = COALESCE (EXCLUDED.c_total_clouds, 'No clouds'),
    vv_horizontal_visibility = COALESCE (EXCLUDED.vv_horizontal_visibility, 0),
    td_temperature_dewpoint = EXCLUDED.td_temperature_dewpoint,
    loaded_ts = EXCLUDED.loaded_ts;

DELETE
FROM etl.weather
WHERE EXISTS (SELECT 1 FROM etl.weather_new);

INSERT INTO etl.weather(loaded_ts)
SELECT ts2
FROM etl.weather_new
WHERE EXISTS (SELECT 1 FROM etl.weather_new);