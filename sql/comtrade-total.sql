SELECT 
    period,
    ROUND(SUM(CASE WHEN flowCode = 'X' THEN fobvalue ELSE 0 END) / 1e9, 2) AS export_value_bln,
    ROUND(SUM(CASE WHEN flowCode = 'M' THEN fobvalue ELSE 0 END) / 1e9, 2) AS import_value_bln
FROM comtrade_data
GROUP BY period
ORDER BY period;