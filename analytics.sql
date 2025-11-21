-- Coldest cities right now
SELECT city, temperature, weather_description
FROM raw_weather
WHERE recorded_at > NOW() - INTERVAL '1 hour'
ORDER BY temperature ASC
LIMIT 10;

-- Average temperature by weather condition
SELECT weather_description, 
       ROUND(AVG(temperature)::numeric, 2) as avg_temp,
       COUNT(*) as occurrences
FROM raw_weather
GROUP BY weather_description
ORDER BY occurrences DESC;

-- Temperature range across Germany
SELECT 
    MIN(temperature) as coldest,
    MAX(temperature) as warmest,
    MAX(temperature) - MIN(temperature) as temp_range
FROM raw_weather
WHERE recorded_at > NOW() - INTERVAL '1 hour';