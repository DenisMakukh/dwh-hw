CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.flights (
    year INT,
    quarter INT,
    month INT,
    flight_date DATE NOT NULL,
    dep_time TIME,
    crs_dep_time TIME NOT NULL,
    air_time FLOAT,
    dep_delay_minutes FLOAT,
    cancelled INT,
    cancellation_code TEXT,
    weather_delay FLOAT,
    reporting_airline TEXT,
    tail_number TEXT,
    flight_number INT NOT NULL,
    distance FLOAT,
    origin TEXT NOT NULL,
    dest TEXT NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    PRIMARY KEY (flight_date, crs_dep_time, flight_number, origin, dest)
);

CREATE TABLE IF NOT EXISTS dds.flights_load_control (
    id SERIAL PRIMARY KEY,
    last_loaded_ts TIMESTAMP NOT NULL DEFAULT '1970-01-01'
);


INSERT INTO dds.flights_load_control (last_loaded_ts)
VALUES ('1970-01-01');



INSERT INTO dds.flights (year, quarter, month, flight_date, dep_time, crs_dep_time, air_time,
                         dep_delay_minutes, cancelled, cancellation_code, weather_delay,
                         reporting_airline, tail_number, flight_number, distance, origin, dest, valid_from)
SELECT year,
       quarter,
       month,
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
       flight_number::int AS flight_number,
       distance,
       origin,
       dest,
       loaded_ts AS valid_from
FROM stg.flights
WHERE loaded_ts > TIMESTAMP '2022-04-30 23:59:59'
ON CONFLICT (flight_date, crs_dep_time, flight_number, origin, dest) DO UPDATE
    SET year = EXCLUDED.year,
        quarter = EXCLUDED.quarter,
        month = EXCLUDED.month,
        dep_time = EXCLUDED.dep_time,
        air_time = EXCLUDED.air_time,
        dep_delay_minutes = EXCLUDED.dep_delay_minutes,
        cancelled = EXCLUDED.cancelled,
        cancellation_code = EXCLUDED.cancellation_code,
        weather_delay = EXCLUDED.weather_delay,
        reporting_airline = EXCLUDED.reporting_airline,
        tail_number = EXCLUDED.tail_number,
        distance = EXCLUDED.distance,
        valid_from = EXCLUDED.valid_from;

SELECT MAX(valid_from) AS new_max_loaded_ts
FROM dds.flights;

DELETE FROM dds.flights_load_control;
INSERT INTO dds.flights_load_control (last_loaded_ts) VALUES ('2022-05-01 10:00:00');



CREATE TABLE IF NOT EXISTS dds.weather (
    icao_code TEXT NOT NULL,
    local_datetime TIMESTAMP NOT NULL,
    t_air_temperature NUMERIC(3,1),
    p0_sea_lvl NUMERIC(4,1),
    p_station_lvl NUMERIC(4,1),
    u_humidity INT,
    dd_wind_direction TEXT,
    ff_wind_speed INT,
    ff10_max_gust_value INT,
    ww_present TEXT,
    ww_recent TEXT,
    c_total_clouds TEXT,
    vv_horizontal_visibility NUMERIC(3,1),
    td_temperature_dewpoint NUMERIC(3,1),
    valid_from TIMESTAMP NOT NULL,
    PRIMARY KEY (icao_code, local_datetime)
);


CREATE TABLE IF NOT EXISTS dds.weather_load_control (
    id SERIAL PRIMARY KEY,
    last_loaded_ts TIMESTAMP NOT NULL DEFAULT '1970-01-01'
);

INSERT INTO dds.weather_load_control (last_loaded_ts)
VALUES ('1970-01-01');


INSERT INTO dds.weather (
    icao_code,
    local_datetime,
    t_air_temperature,
    p0_sea_lvl,
    p_station_lvl,
    u_humidity,
    dd_wind_direction,
    ff_wind_speed,
    ff10_max_gust_value,
    ww_present,
    ww_recent,
    c_total_clouds,
    vv_horizontal_visibility,
    td_temperature_dewpoint,
    valid_from
)
SELECT
    icao_code,
    local_datetime::timestamp,
    t_air_temperature::numeric(3,1),
    p0_sea_lvl::numeric(4,1),
    p_station_lvl::numeric(4,1),
    u_humidity::int,
    dd_wind_direction,
    ff_wind_speed::int,
    ff10_max_gust_value::int,
    ww_present,
    ww_recent,
    c_total_clouds,
    vv_horizontal_visibility::numeric(3,1),
    td_temperature_dewpoint::numeric(3,1),
    loaded_ts AS valid_from
FROM stg.weather
WHERE loaded_ts > TIMESTAMP '2022-04-30 23:59:59'
ON CONFLICT (icao_code, local_datetime) DO UPDATE
    SET t_air_temperature = EXCLUDED.t_air_temperature,
        p0_sea_lvl = EXCLUDED.p0_sea_lvl,
        p_station_lvl = EXCLUDED.p_station_lvl,
        u_humidity = EXCLUDED.u_humidity,
        dd_wind_direction = EXCLUDED.dd_wind_direction,
        ff_wind_speed = EXCLUDED.ff_wind_speed,
        ff10_max_gust_value = EXCLUDED.ff10_max_gust_value,
        ww_present = EXCLUDED.ww_present,
        ww_recent = EXCLUDED.ww_recent,
        c_total_clouds = EXCLUDED.c_total_clouds,
        vv_horizontal_visibility = EXCLUDED.vv_horizontal_visibility,
        td_temperature_dewpoint = EXCLUDED.td_temperature_dewpoint,
        valid_from = EXCLUDED.valid_from;


SELECT MAX(valid_from) AS new_max_loaded_ts
FROM dds.weather;




DELETE FROM dds.weather_load_control;
INSERT INTO dds.weather_load_control (last_loaded_ts) VALUES ('2022-05-01 10:00:00');
