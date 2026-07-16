# -*- coding: utf-8 -*-
"""Golden snapshot итоговой БД db/unified_trade_data.duckdb.

Снимает контрольные метрики: число строк, диапазон дат, агрегаты
STOIM/NETTO/KOL по стране и направлению, долю TYPE='pred', долю
незамапленного EDIZM, число строк справочных таблиц. Сохраняет в JSON.

Используется как страховка при структурном рефакторинге: снять baseline
до правок, после правок прогнать compare — любое расхождение агрегатов
означает, что рефакторинг изменил данные.

Использование:
    python scripts/golden_snapshot.py snapshot [--db PATH] [--out FILE]
    python scripts/golden_snapshot.py compare  [--db PATH] [--baseline FILE]

По умолчанию baseline хранится в db_snapshots/baseline.json (трекается git,
в отличие от db/). compare завершается с кодом 1 при любом расхождении.
"""
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"
DEFAULT_BASELINE = PROJECT_ROOT / "db_snapshots" / "baseline.json"
MAIN_TABLE = "unified_trade_data"
# Таблицы, для которых фиксируем только число строк
AUX_TABLES = ["fizob_index", "country_reference", "hs4_reference", "tnved_reference"]
# Относительный допуск для сумм DOUBLE (параллельная агрегация DuckDB
# не гарантирует битовую воспроизводимость порядка сложения)
FLOAT_RTOL = 1e-9


def take_snapshot(db_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        total_rows = con.execute(f"SELECT COUNT(*) FROM {MAIN_TABLE}").fetchone()[0]
        period_min, period_max = con.execute(
            f"SELECT MIN(PERIOD), MAX(PERIOD) FROM {MAIN_TABLE}"
        ).fetchone()
        unique_countries = con.execute(
            f"SELECT COUNT(DISTINCT STRANA) FROM {MAIN_TABLE}"
        ).fetchone()[0]

        rows_by_source = dict(
            con.execute(
                f"SELECT SOURCE, COUNT(*) FROM {MAIN_TABLE} GROUP BY SOURCE ORDER BY SOURCE"
            ).fetchall()
        )
        type_counts = dict(
            con.execute(
                f"SELECT COALESCE(TYPE, 'fact'), COUNT(*) FROM {MAIN_TABLE} "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        )
        pred_rows = type_counts.get("pred", 0)

        edizm_unmapped_rows = con.execute(
            f"SELECT COUNT(*) FROM {MAIN_TABLE} "
            "WHERE EDIZM IS NULL OR TRIM(EDIZM) = ''"
        ).fetchone()[0]

        by_country_napr = [
            {
                "STRANA": strana,
                "NAPR": napr,
                "rows": rows,
                "stoim_sum": stoim,
                "netto_sum": netto,
                "kol_sum": kol,
            }
            for strana, napr, rows, stoim, netto, kol in con.execute(
                f"""
                SELECT STRANA, NAPR, COUNT(*),
                       SUM(STOIM), SUM(NETTO), SUM(KOL)
                FROM {MAIN_TABLE}
                GROUP BY STRANA, NAPR
                ORDER BY STRANA, NAPR
                """
            ).fetchall()
        ]

        existing_tables = {
            name
            for (name,) in con.execute(
                "SELECT table_name FROM duckdb_tables()"
            ).fetchall()
        }
        aux_row_counts = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in AUX_TABLES
            if table in existing_tables
        }
    finally:
        con.close()

    return {
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "table": MAIN_TABLE,
        "total_rows": total_rows,
        "period_min": str(period_min),
        "period_max": str(period_max),
        "unique_countries": unique_countries,
        "rows_by_source": rows_by_source,
        "type_counts": type_counts,
        "pred_share_pct": round(pred_rows / total_rows * 100, 4) if total_rows else 0.0,
        "edizm_unmapped_rows": edizm_unmapped_rows,
        "edizm_unmapped_share_pct": (
            round(edizm_unmapped_rows / total_rows * 100, 4) if total_rows else 0.0
        ),
        "by_country_napr": by_country_napr,
        "aux_row_counts": aux_row_counts,
    }


def _floats_equal(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=FLOAT_RTOL, abs_tol=1e-6)


def compare_snapshots(baseline: dict, current: dict) -> list:
    """Возвращает список строк-расхождений (пустой = совпадение)."""
    diffs = []

    scalar_keys = [
        "total_rows",
        "period_min",
        "period_max",
        "unique_countries",
        "edizm_unmapped_rows",
    ]
    for key in scalar_keys:
        if baseline.get(key) != current.get(key):
            diffs.append(f"{key}: baseline={baseline.get(key)!r} -> current={current.get(key)!r}")

    for key in ["rows_by_source", "type_counts", "aux_row_counts"]:
        base_map, cur_map = baseline.get(key, {}), current.get(key, {})
        for name in sorted(set(base_map) | set(cur_map)):
            if base_map.get(name) != cur_map.get(name):
                diffs.append(
                    f"{key}[{name}]: baseline={base_map.get(name)!r} -> current={cur_map.get(name)!r}"
                )

    base_groups = {(g["STRANA"], g["NAPR"]): g for g in baseline.get("by_country_napr", [])}
    cur_groups = {(g["STRANA"], g["NAPR"]): g for g in current.get("by_country_napr", [])}
    for group_key in sorted(set(base_groups) | set(cur_groups), key=str):
        base_group, cur_group = base_groups.get(group_key), cur_groups.get(group_key)
        if base_group is None:
            diffs.append(f"by_country_napr {group_key}: появилась новая группа ({cur_group['rows']} строк)")
            continue
        if cur_group is None:
            diffs.append(f"by_country_napr {group_key}: группа исчезла (было {base_group['rows']} строк)")
            continue
        if base_group["rows"] != cur_group["rows"]:
            diffs.append(
                f"by_country_napr {group_key} rows: {base_group['rows']} -> {cur_group['rows']}"
            )
        for metric in ["stoim_sum", "netto_sum", "kol_sum"]:
            if not _floats_equal(base_group[metric], cur_group[metric]):
                diffs.append(
                    f"by_country_napr {group_key} {metric}: "
                    f"{base_group[metric]!r} -> {cur_group[metric]!r}"
                )
    return diffs


def print_summary(snap: dict) -> None:
    print(f"Таблица {snap['table']}: {snap['total_rows']:,} строк, "
          f"{snap['unique_countries']} стран, {snap['period_min']} — {snap['period_max']}")
    print(f"TYPE='pred': {snap['type_counts'].get('pred', 0):,} строк ({snap['pred_share_pct']}%)")
    print(f"EDIZM не замаплен: {snap['edizm_unmapped_rows']:,} строк "
          f"({snap['edizm_unmapped_share_pct']}%)")
    print(f"Групп страна×направление: {len(snap['by_country_napr'])}")


def main() -> int:
    # Консоль Windows по умолчанию cp1252 — кириллица в выводе падает
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    subparsers = parser.add_subparsers(dest="command", required=True)

    snap_parser = subparsers.add_parser("snapshot", help="снять snapshot и сохранить в JSON")
    snap_parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    snap_parser.add_argument("--out", type=Path, default=DEFAULT_BASELINE)

    cmp_parser = subparsers.add_parser("compare", help="сверить текущую БД с baseline")
    cmp_parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    cmp_parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)

    args = parser.parse_args()

    if not args.db.exists():
        print(f"БД не найдена: {args.db}")
        return 1

    if args.command == "snapshot":
        snap = take_snapshot(args.db)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(snap, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print_summary(snap)
        print(f"Snapshot сохранён: {args.out}")
        return 0

    if not args.baseline.exists():
        print(f"Baseline не найден: {args.baseline} (сначала запустите snapshot)")
        return 1
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    current = take_snapshot(args.db)
    diffs = compare_snapshots(baseline, current)
    if diffs:
        print(f"РАСХОЖДЕНИЯ с baseline ({args.baseline.name}, снят {baseline['created_at']}):")
        for diff in diffs:
            print(f"  - {diff}")
        return 1
    print(f"OK: текущая БД совпадает с baseline ({args.baseline.name}, "
          f"снят {baseline['created_at']}).")
    print_summary(current)
    return 0


if __name__ == "__main__":
    sys.exit(main())
