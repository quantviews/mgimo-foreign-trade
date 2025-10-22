-- процентное изменение оборота каждого месяца по 2024 году для импорта из Турции с группировкой по TNVED2

WITH monthly_data AS (
  SELECT
    TNVED2,
    DATE_TRUNC('month', PERIOD) AS month,
    SUM(STOIM) AS total_stoim
  FROM unified_trade_data.main.unified_trade_data
  WHERE NAPR = 'ИМ'
    AND STRANA = 'TR'
    AND SOURCE = 'national'
    AND PERIOD BETWEEN DATE '2023-12-01' AND DATE '2024-12-31'
    AND TNVED2 BETWEEN '01' AND '99'
  GROUP BY TNVED2, month
),
monthly_change AS (
  SELECT
    TNVED2,
    month,
    CASE
      WHEN LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY month) = 0 THEN NULL
      ELSE ROUND(
        ((total_stoim - LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY month))
         / LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY month)) * 100, 2)
    END AS percent_change
  FROM monthly_data
)
SELECT
  TNVED2,
  MAX(CASE WHEN month = DATE '2024-01-01' THEN percent_change END) AS "Jan_2024",
  MAX(CASE WHEN month = DATE '2024-02-01' THEN percent_change END) AS "Feb_2024",
  MAX(CASE WHEN month = DATE '2024-03-01' THEN percent_change END) AS "Mar_2024",
  MAX(CASE WHEN month = DATE '2024-04-01' THEN percent_change END) AS "Apr_2024",
  MAX(CASE WHEN month = DATE '2024-05-01' THEN percent_change END) AS "May_2024",
  MAX(CASE WHEN month = DATE '2024-06-01' THEN percent_change END) AS "Jun_2024",
  MAX(CASE WHEN month = DATE '2024-07-01' THEN percent_change END) AS "Jul_2024",
  MAX(CASE WHEN month = DATE '2024-08-01' THEN percent_change END) AS "Aug_2024",
  MAX(CASE WHEN month = DATE '2024-09-01' THEN percent_change END) AS "Sep_2024",
  MAX(CASE WHEN month = DATE '2024-10-01' THEN percent_change END) AS "Oct_2024",
  MAX(CASE WHEN month = DATE '2024-11-01' THEN percent_change END) AS "Nov_2024",
  MAX(CASE WHEN month = DATE '2024-12-01' THEN percent_change END) AS "Dec_2024"
FROM monthly_change
GROUP BY TNVED2
ORDER BY TNVED2;
