"""Обновление справочника ТН ВЭД из классификатора ФНС.

ФТС заморозила публикацию THBED.dbf в феврале 2022 г. вместе с детальной
таможенной статистикой, поэтому исходник metadata/tnved.csv больше не
обновляется. Живой машиночитаемый аналог — классификатор ТНВЭД ФНС
(https://www.nalog.gov.ru/rn77/program/5961290/, архив data.nalog.ru).

Скрипт:
  1) скачивает и разбирает архив (TNVED3 — товарные позиции, TNVED4 —
     подсубпозиции с историей действия);
  2) дописывает в metadata/tnved.csv коды 10-го уровня, которых там нет
     (введённые решениями ЕЭК после февраля 2022 г.);
  3) пишет metadata/tnved_validity.csv — даты действия и признак
     актуальности для каждого кода 10-го уровня;
  4) проставляет колонку SOURCE (fts / fns) — откуда взято наименование.

Существующие строки tnved.csv не переписываются: наименования ФНС даны
в виде фрагментов ЕТТ с отступами ("- - чистопородные племенные животные"),
а промежуточные строки субпозиций в выгрузке отсутствуют, поэтому
восстановить полное наименование лучше, чем во ФТС-версии, нельзя.
Коды, вышедшие из применения, не удаляются: исторические периоды данных
по ним есть.

Запуск: python scripts/refresh_tnved_from_fns.py [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import re
import struct
import urllib.request
import zipfile
from pathlib import Path

FNS_URL = "https://data.nalog.ru/files/tnved/tnved.zip"
FNS_REFERER = "https://www.nalog.gov.ru/rn77/program/5961290/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TNVED_CSV = PROJECT_ROOT / "metadata" / "tnved.csv"
VALIDITY_CSV = PROJECT_ROOT / "metadata" / "tnved_validity.csv"
THBED_DBF = PROJECT_ROOT / "metadata" / "THBED.dbf"

SOURCE_FTS = "fts"
SOURCE_FNS = "fns"

DASH_PREFIX = re.compile(r"^((?:-[\s\xa0]*)*)")


def download() -> dict[str, list[list[str]]]:
    """Скачивает архив ФНС и возвращает разобранные записи по файлам."""
    req = urllib.request.Request(
        FNS_URL, headers={"User-Agent": USER_AGENT, "Referer": FNS_REFERER}
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        blob = resp.read()

    out = {}
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for info in zf.infolist():
            text = zf.read(info).decode("cp866")
            lines = text.split("\r\n")
            out[info.filename.upper()] = [
                line.split("|") for line in lines[1:] if line.strip()
            ]
    return out


def read_dbf_codes(path: Path) -> set[str]:
    """Коды из исходного справочника ФТС — по ним размечается происхождение строк."""
    if not path.exists():
        return set()
    with path.open("rb") as f:
        n_records, header_len, record_len = struct.unpack("<IHH", f.read(12)[4:12])
        f.seek(header_len)
        codes = set()
        for _ in range(n_records):
            record = f.read(record_len)
            if not record or record[:1] == b"*":
                continue
            codes.add(record[1:65].decode("cp866").strip())
    return codes


def parse_date(value: str):
    value = value.strip()
    return dt.datetime.strptime(value, "%d.%m.%Y").date() if value else None


def is_active(row: list[str], start: int, end: int, on: dt.date) -> bool:
    a, b = parse_date(row[start]), parse_date(row[end])
    return (a is None or a <= on) and (b is None or b >= on)


def depth(name: str) -> int:
    return DASH_PREFIX.match(name).group(1).count("-")


def fragment(name: str) -> str:
    return DASH_PREFIX.sub("", name, count=1).strip()


def significant(code: str) -> str:
    """Значащая часть кода — без хвостовых нулей (для проверки родства)."""
    return code.rstrip("0") or code[:2]


def build_names(headings: dict[str, str], subs: dict[str, str]) -> dict[str, str]:
    """Собирает наименование из цепочки предков внутри товарной позиции.

    В выгрузке ФНС есть только конечные 10-значные коды, промежуточные
    строки субпозиций отсутствуют, поэтому цепочка местами неполная.
    Предком считается только строка, чей значащий код является префиксом
    текущего, иначе к коду приклеился бы соседний, а не родительский текст.
    """
    names: dict[str, str] = {}
    by_heading: dict[str, list[str]] = {}
    for code in sorted(subs):
        by_heading.setdefault(code[:4], []).append(code)

    for heading, codes in by_heading.items():
        heading_name = headings.get(heading, "")
        stack: list[tuple[int, str, str]] = []
        for code in codes:
            raw = subs[code]
            level, text = depth(raw), fragment(raw)
            stack = [
                item
                for item in stack
                if item[0] < level and code.startswith(significant(item[1]))
            ]
            if level > 0 and heading_name:
                parts = [item[2] for item in stack] + [text]
                names[code] = f"{heading_name}: " + ": ".join(p for p in parts if p)
            else:
                names[code] = text or heading_name
            stack.append((level, code, text))
    return names


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--date",
        default=dt.date.today().isoformat(),
        help="дата, на которую определяется актуальность кодов",
    )
    args = ap.parse_args()
    on = dt.date.fromisoformat(args.date)

    files = download()
    headings = {
        r[0] + r[1]: r[2].strip()
        for r in files["TNVED3.TXT"]
        if is_active(r, 3, 4, on)
    }
    subs = {
        r[0] + r[1] + r[2]: r[3].strip()
        for r in files["TNVED4.TXT"]
        if is_active(r, 4, 5, on)
    }
    validity = {}
    for r in files["TNVED4.TXT"]:
        code = r[0] + r[1] + r[2]
        if is_active(r, 4, 5, on):
            validity[code] = (r[4].strip(), r[5].strip(), "active")
        elif code not in validity:
            validity[code] = (r[4].strip(), r[5].strip(), "obsolete")

    names = build_names(headings, subs)
    print(f"ФНС: {len(headings)} позиций, {len(subs)} действующих подсубпозиций")

    rows = list(csv.DictReader(TNVED_CSV.open(encoding="utf-8")))
    known10 = {r["KOD"] for r in rows if r["level"] == "10"}
    added = sorted(set(subs) - known10)
    obsolete = sorted(known10 - set(subs))

    for code in added:
        rows.append({"KOD": code, "NAME": names[code].upper(), "level": "10"})
    rows.sort(key=lambda r: r["KOD"])

    # Происхождение наименования: строки исходного справочника ФТС против
    # добавленных из классификатора ФНС. Считается от THBED.dbf, поэтому
    # повторный запуск скрипта не смещает разметку.
    fts_codes = read_dbf_codes(THBED_DBF)
    for r in rows:
        r["SOURCE"] = (
            r.get("SOURCE")
            or (SOURCE_FTS if not fts_codes or r["KOD"] in fts_codes else SOURCE_FNS)
        )

    with TNVED_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(["KOD", "NAME", "level", "SOURCE"])
        for r in rows:
            w.writerow([r["KOD"], r["NAME"], int(r["level"]), r["SOURCE"]])

    with VALIDITY_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(["KOD", "VALID_FROM", "VALID_TO", "STATUS"])
        for code in sorted(set(validity) | known10):
            vf, vt, status = validity.get(code, ("", "", "unknown"))
            w.writerow([code, vf, vt, status])

    from collections import Counter
    print("источник наименований:", dict(Counter(r["SOURCE"] for r in rows)))
    print(f"добавлено кодов 10-го уровня: {len(added)}")
    print(f"есть у нас, но не действуют на {on}: {len(obsolete)}")
    print(f"всего строк в tnved.csv: {len(rows)}")


if __name__ == "__main__":
    main()
