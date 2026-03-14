CREATE VIEW avg_temp_por_dispositivo AS
SELECT
    room_id AS device_id,
    ROUND(AVG(temp), 2) AS avg_temp
FROM iot_temperature_readings
GROUP BY room_id;

CREATE VIEW leituras_por_hora AS
SELECT
    EXTRACT(HOUR FROM noted_date) AS hora,
    COUNT(*) AS contagem
FROM iot_temperature_readings
GROUP BY EXTRACT(HOUR FROM noted_date)
ORDER BY hora;

CREATE VIEW temp_max_min_por_dia AS
SELECT
    DATE(noted_date) AS data,
    MAX(temp) AS temp_max,
    MIN(temp) AS temp_min
FROM iot_temperature_readings
GROUP BY DATE(noted_date)
ORDER BY data;