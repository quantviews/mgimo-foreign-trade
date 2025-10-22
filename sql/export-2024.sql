-- статистика по экспорту по странам за период
SELECT *
FROM (
  SELECT
    TNVED2,
    STRANA,
    SUM(STOIM) AS total_stoim
  FROM unified_trade_data.main.unified_trade_data
  WHERE PERIOD BETWEEN DATE '2024-01-01' AND DATE '2024-12-01'
    AND NAPR = 'ЭК'
    AND SOURCE = 'national'    
    AND TNVED2 BETWEEN '01' AND '99'
  GROUP BY TNVED2, STRANA
)
PIVOT (
  SUM(total_stoim)
  FOR STRANA IN ('TR', 'CN', 'IN')
)
ORDER BY TNVED2;
