-- Create table dim_dates
CREATE TABLE `mercadolibre-test-395519.movil.dim_dates` (
    Date_id INTEGER,
    Date DATE,
    Day INT64,
    Month INT64,
    Year INT64,
    Quarter INT64,
    Semester INT64,
    Day_name STRING
);

-- Create dates
INSERT INTO `mercadolibre-test-395519.movil.dim_dates`
SELECT
    (EXTRACT(YEAR FROM date)*100 + EXTRACT(MONTH FROM date))*100 + EXTRACT(DAY FROM date) AS date_id,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    CASE WHEN EXTRACT(MONTH FROM date) <= 6 THEN 1 ELSE 2 END AS semester,
    FORMAT_DATE('%A', date) AS day_name
FROM (
    SELECT GENERATE_DATE_ARRAY(DATE '2023-01-01', DATE '2023-12-31', INTERVAL 1 DAY) AS date_array
)
CROSS JOIN UNNEST(date_array) AS date;