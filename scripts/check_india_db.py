# -*- coding: utf-8 -*-
"""Проверка данных Индии в итоговой БД: STOIM по месяцам (последние 18 мес)."""
import duckdb
from pathlib import Path

db_path = Path(__file__).resolve().parent.parent / "db" / "unified_trade_data.duckdb"
if not db_path.exists():
    print(f"БД не найдена: {db_path}")
    exit(1)

con = duckdb.connect(str(db_path), read_only=True)

q = """
SELECT
  strftime(PERIOD, '%Y-%m') AS month,
  SUM(STOIM) AS total_stoim,
  COUNT(*) AS rows
FROM unified_trade_data
WHERE STRANA = 'IN'
GROUP BY strftime(PERIOD, '%Y-%m')
ORDER BY month DESC
LIMIT 18
"""
rows = con.execute(q).fetchall()
con.close()

print("Индия (STRANA='IN') в unified_trade_data — STOIM по месяцам (тыс. USD):")
print(f"{'month':<8} {'total_stoim':>18} {'rows':>8}")
print("-" * 36)
for month, total_stoim, row_count in rows:
    print(f"{month:<8} {total_stoim:>18,.0f} {row_count:>8}")
print()
# Проверка на провал размерности
if len(rows) >= 2:
    last_val = float(rows[0][1])
    prev_val = float(rows[1][1])
    ratio = prev_val / last_val if last_val else 0
    if ratio > 100 or (0 < ratio < 0.01):
        print(f"ВНИМАНИЕ: резкий перепад между {rows[1][0]} и {rows[0][0]} (отношение ~{ratio:.1f}x)")
    else:
        print("Резких скачков размерности между последними месяцами не видно.")
