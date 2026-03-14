DROP TABLE IF EXISTS iot_temperature_readings CASCADE;

CREATE TABLE iot_temperature_readings (
    id TEXT PRIMARY KEY,
    room_id VARCHAR(100),
    noted_date TIMESTAMP,
    temp NUMERIC(10,2),
    location_type VARCHAR(10)
);