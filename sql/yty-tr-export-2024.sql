-- процентное изменение оборота год к году с 2014 по 2024 для экспорта в Турцию с группировкой по TNVED2
WITH yearly_data AS (
  SELECT
    TNVED2,
    EXTRACT(year FROM PERIOD) AS year,
    SUM(STOIM) AS total_stoim
  FROM unified_trade_data.main.unified_trade_data
  WHERE NAPR = 'ЭК'
    AND STRANA = 'TR'
    AND SOURCE = 'national'
    AND PERIOD BETWEEN DATE '2013-01-01' AND DATE '2024-12-31'
    AND TNVED2 BETWEEN '01' AND '99'
  GROUP BY TNVED2, year
),
yearly_change AS (
  SELECT
    TNVED2,
    year,
    CASE
      WHEN LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY year) = 0 THEN NULL
      ELSE ROUND(
        ((total_stoim - LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY year))
         / LAG(total_stoim) OVER (PARTITION BY TNVED2 ORDER BY year)) * 100, 2)
    END AS percent_change
  FROM yearly_data
)
SELECT
  TNVED2,
  MAX(CASE WHEN year = 2014 THEN percent_change END) AS "2014_vs_2013",
  MAX(CASE WHEN year = 2015 THEN percent_change END) AS "2015_vs_2014",
  MAX(CASE WHEN year = 2016 THEN percent_change END) AS "2016_vs_2015",
  MAX(CASE WHEN year = 2017 THEN percent_change END) AS "2017_vs_2016",
  MAX(CASE WHEN year = 2018 THEN percent_change END) AS "2018_vs_2017",
  MAX(CASE WHEN year = 2019 THEN percent_change END) AS "2019_vs_2018",
  MAX(CASE WHEN year = 2020 THEN percent_change END) AS "2020_vs_2019",
  MAX(CASE WHEN year = 2021 THEN percent_change END) AS "2021_vs_2020",
  MAX(CASE WHEN year = 2022 THEN percent_change END) AS "2022_vs_2021",
  MAX(CASE WHEN year = 2023 THEN percent_change END) AS "2023_vs_2022",
  MAX(CASE WHEN year = 2024 THEN percent_change END) AS "2024_vs_2023"
FROM yearly_change
GROUP BY TNVED2
ORDER BY TNVED2;
