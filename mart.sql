CREATE TABLE mart.fact_departure (
    flight_date DATE NOT NULL,
    origin TEXT NOT NULL,
    dest TEXT NOT NULL,
    ww_present TEXT,
    dep_time TIME,
    crs_dep_time TIME NOT NULL,
    flight_number INT NOT NULL,
    distance FLOAT8,
    tail_number TEXT,
    reporting_airline TEXT,
    dep_delay_minutes FLOAT8,
    cancelled INT,
    cancellation_code TEXT,
    t_air_temperature NUMERIC(3, 1),
    ff10_max_gust_value INT,
    ff_wind_speed INT,
    air_time FLOAT8,
    p0_sea_lvl NUMERIC(4, 1),
    p_station_lvl NUMERIC(4, 1),
    u_humidity INT,
    dd_wind_direction TEXT,
    c_total_clouds TEXT,
    vv_horizontal_visibility NUMERIC(3, 1),
    td_temperature_dewpoint NUMERIC(3, 1),
    author INT,
    loaded_ts TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fact_departure_pkey PRIMARY KEY (origin, dest, crs_dep_time, flight_number)
);

INSERT INTO mart.fact_departure (
    flight_date,
    origin,
    dest,
    ww_present,
    dep_time,
    crs_dep_time,
    flight_number,
    distance,
    tail_number,
    reporting_airline,
    dep_delay_minutes,
    cancelled,
    cancellation_code,
    t_air_temperature,
    ff10_max_gust_value,
    ff_wind_speed,
    air_time,
    p0_sea_lvl,
    p_station_lvl,
    u_humidity,
    dd_wind_direction,
    c_total_clouds,
    vv_horizontal_visibility,
    td_temperature_dewpoint,
    author,
    loaded_ts
)
SELECT
    f.flight_date,
    f.origin,
    f.dest,
    w.ww_present,
    f.dep_time,
    f.crs_dep_time,
    f.flight_number,
    f.distance,
    f.tail_number,
    f.reporting_airline,
    f.dep_delay_minutes,
    f.cancelled,
    f.cancellation_code,
    w.t_air_temperature,
    w.ff10_max_gust_value,
    w.ff_wind_speed,
    f.air_time,
    w.p0_sea_lvl,
    w.p_station_lvl,
    w.u_humidity,
    w.dd_wind_direction,
    w.c_total_clouds,
    w.vv_horizontal_visibility,
    w.td_temperature_dewpoint,
    52 AS author,
    NOW() AS loaded_ts
FROM
    dds.flights f
LEFT JOIN LATERAL (
    SELECT *
    FROM dds.weather w
    WHERE 'K' || f.origin = w.icao_code
    ORDER BY ABS(EXTRACT(EPOCH FROM (w.local_datetime - (f.flight_date + f.crs_dep_time)))) ASC
    LIMIT 1
) w ON TRUE
WHERE
    f.dest IS NOT NULL
ON CONFLICT DO NOTHING;