#!/usr/bin/env python3
"""Загрузка помесячных данных UN Comtrade о торговле с Россией.

Порт скрипта MirrorTrade/comtrade-data.py в пайплайн проекта. Отличия от
оригинала:

  * ключ подписки берётся из COMTRADE_API_KEY (окружение или .env), а не
    зашит в исходник;
  * результат сразу пишется в data_raw/comtrade_data/YYYY-MM.parquet — формат,
    который читает src/merge_comtrade_to_duckdb.py, без промежуточного CSV;
  * схема нового файла приводится к схеме уже лежащих рядом, иначе UNION ALL
    при сборке comtrade.db падает на несовпадении типов у колонок, которые при
    includeDesc=False приходят пустыми (cmdDesc, aggrLevel, isLeaf);
  * текущий, ещё не закрытый месяц не качается: файл за него сохранился бы
    частичным и больше никогда не обновился;
  * появился пересмотр уже скачанных месяцев — см. ниже.

Про пересмотры. Страны досылают и правят отчётность годами: данные Андорры за
январь 2024 впервые опубликованы 26.02.2024, а последний раз — 03.08.2026.
На июнь 2025 после нашей загрузки отчётность пересмотрел 51 репортёр из 95, на
июнь 2023 — 9 из 120. Поэтому «перекачать последние N месяцев» не работает: и
глубина, и охват меняются от месяца к месяцу.

Вместо этого по каждому сохранённому месяцу хранится манифест
(_manifest.json): какие страны в файле и какой у их выгрузки был
datasetChecksum. Перед обновлением манифест сверяется со справочником
доступности — он публичный, ключа и квоты не требует. Скачиваются только те
страны, у которых чек-сумма изменилась или которых в файле не было, и их
строки замещаются в существующем parquet. Для файлов, скачанных до появления
манифеста, признаком устаревания служит lastReleased позже времени
модификации файла.

Запуск:
    python src/collectors/comtrade_collector.py --check      # что устарело, без загрузки и без ключа
    python src/collectors/comtrade_collector.py              # догрузить недостающие месяцы
    python src/collectors/comtrade_collector.py --refresh    # плюс пересмотренные страны
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from collectors._base import get_project_root, setup_logging, valid_year  # noqa: E402

logger = setup_logging(__name__)

RUSSIA_M49 = "643"
OUTPUT_SUBDIR = Path("data_raw") / "comtrade_data"
MANIFEST_NAME = "_manifest.json"
DEFAULT_START_YEAR = 2019
REQUEST_PAUSE_SECONDS = 3
QUOTA_EXIT_CODE = 2


class QuotaExceeded(RuntimeError):
    """Лимит запросов к API исчерпан — прогон прерывается до следующего раза."""


@dataclass
class PeriodPlan:
    """Что нужно сделать с одним месяцем."""

    year: int
    month: int
    missing: bool = False
    reporters: list[str] = field(default_factory=list)
    new_reporters: list[str] = field(default_factory=list)
    revised_reporters: list[str] = field(default_factory=list)

    @property
    def code(self) -> str:
        return f"{self.year}{self.month:02d}"

    @property
    def name(self) -> str:
        return f"{self.year}-{self.month:02d}"


def load_api_key(project_root: Path) -> str:
    key = os.environ.get("COMTRADE_API_KEY", "").strip()
    if key:
        return key
    env_file = project_root / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "COMTRADE_API_KEY":
                return value.strip().strip('"').strip("'")
    raise SystemExit(
        "COMTRADE_API_KEY не найден. Положите ключ подписки Comtrade в окружение "
        "или в .env в корне проекта."
    )


def last_closed_month(today: date) -> tuple[int, int]:
    """Последний завершившийся месяц: за текущий данные заведомо неполны."""
    return (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)


def parse_period(value: str) -> tuple[int, int]:
    """argparse type: месяц в виде YYYY-MM."""
    try:
        year, month = value.split("-")
        year, month = int(year), int(month)
        if not 1 <= month <= 12:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError(f"Ожидается месяц в виде YYYY-MM, получено {value!r}")
    return year, month


def enumerate_periods(start_year: int, end: tuple[int, int]) -> list[tuple[int, int]]:
    end_year, end_month = end
    periods: list[tuple[int, int]] = []
    for year in range(start_year, end_year + 1):
        last_month = end_month if year == end_year else 12
        periods.extend((year, month) for month in range(1, last_month + 1))
    return periods


def period_file(output_dir: Path, year: int, month: int) -> Path:
    return output_dir / f"{year}-{month:02d}.parquet"


def read_manifest(output_dir: Path) -> dict:
    path = output_dir / MANIFEST_NAME
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("Манифест повреждён (%s), считаем его пустым", exc)
        return {}


def write_manifest(output_dir: Path, manifest: dict) -> None:
    path = output_dir / MANIFEST_NAME
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def availability_index(availability: pd.DataFrame) -> dict[str, dict]:
    """reporterCode -> {checksum, lastReleased} из справочника доступности."""
    if availability is None or availability.empty:
        return {}
    return {
        str(row["reporterCode"]): {
            "checksum": str(row.get("datasetChecksum", "")),
            "lastReleased": str(row.get("lastReleased", "")),
        }
        for _, row in availability.iterrows()
    }


def _released_after(last_released: str, moment: datetime | None) -> bool:
    if moment is None or not last_released:
        return True
    # Сравниваем как Timestamp: у lastReleased есть наносекунды, и перевод в
    # datetime их отбрасывает с предупреждением.
    stamp = pd.to_datetime(last_released, errors="coerce")
    return bool(pd.notna(stamp) and stamp > pd.Timestamp(moment))


def stale_reporters(
    available: dict[str, dict],
    recorded: dict[str, dict] | None,
    file_mtime: datetime | None,
    present_in_file: set[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Кого нужно перекачать: (отсутствующие в файле, пересмотренные).

    Страна попадает в первый список, если её данные доступны, но в файле их
    нет — так бывает и когда месяц скачали до того, как страна отчиталась
    (2026-06 сохранён с единственным репортёром из 42 доступных сейчас), и
    когда запрос к ней в прошлый раз не прошёл.

    Во второй список страна попадает, когда её datasetChecksum изменился с
    момента загрузки. Для файлов, скачанных до появления манифеста, чек-суммы
    неизвестны, и признаком служит lastReleased позже времени модификации
    файла — грубее, но не требует перекачивать всё ради первого прогона.
    """
    recorded = recorded or {}
    present = present_in_file if present_in_file is not None else set(recorded)
    new, revised = [], []
    for code, meta in available.items():
        if code not in present:
            new.append(code)
            continue
        previous = recorded.get(code)
        if previous is None:
            if _released_after(meta["lastReleased"], file_mtime):
                revised.append(code)
        elif previous.get("checksum") != meta["checksum"]:
            revised.append(code)
    return sorted(new), sorted(revised)


def splice_reporters(
    existing: pd.DataFrame, fresh: pd.DataFrame, reporters: list[str]
) -> pd.DataFrame:
    """Замещает строки перечисленных стран свежими, остальные оставляет как есть."""
    if existing.empty:
        return fresh
    codes = {str(code) for code in reporters}
    kept = existing[~existing["reporterCode"].astype(str).isin(codes)]
    if fresh.empty:
        return kept
    return pd.concat([kept, fresh], ignore_index=True)


def active_reporters(api) -> pd.DataFrame:
    """Действующие страны-репортёры, кроме самой России.

    Справочник отдаётся без ключа подписки, поэтому шаг работает и при
    исчерпанной квоте.
    """
    reference = api.getReference("partner")
    if reference is None or reference.empty:
        raise RuntimeError("Comtrade вернул пустой справочник партнёров")
    reporters = reference[
        reference["entryExpiredDate"].isna()
        & (reference["isGroup"] == False)  # noqa: E712 — сравнение в pandas, не с None
        & (reference["id"] != RUSSIA_M49)
    ]
    if reporters.empty:
        raise RuntimeError("После фильтрации не осталось ни одного репортёра")
    return reporters


def fetch_availability(api, period: str) -> pd.DataFrame | None:
    """Справочник доступности за период. Публичный вызов: без ключа и квоты."""
    return api.getFinalDataAvailability(
        None, typeCode="C", freqCode="M", clCode="HS", period=period, reporterCode=None
    )


def _is_quota_error(exc: Exception) -> bool:
    return "quota" in str(exc).lower()


def fetch_reporter(api, key: str, period: str, reporter_id: str) -> list[pd.DataFrame]:
    """Экспорт и импорт одной страны в торговле с Россией за месяц."""
    frames = []
    for flow_code in ("X", "M"):
        try:
            data = api.getFinalData(
                key,
                typeCode="C",
                freqCode="M",
                clCode="HS",
                period=period,
                reporterCode=reporter_id,
                cmdCode="ALL",
                flowCode=flow_code,
                partnerCode=RUSSIA_M49,
                partner2Code=None,
                customsCode=None,
                motCode=None,
                maxRecords=50000,
                includeDesc=False,
            )
        except Exception as exc:
            if _is_quota_error(exc):
                raise QuotaExceeded(f"квота исчерпана на репортёре {reporter_id} за {period}") from exc
            logger.warning("  репортёр %s (%s): %s", reporter_id, flow_code, exc)
            continue
        if data is not None and not data.empty:
            frames.append(data)
        time.sleep(REQUEST_PAUSE_SECONDS)
    return frames


def reference_schema(output_dir: Path) -> "pd.Series | None":
    """Типы колонок из любого уже сохранённого файла."""
    existing = sorted(p for p in output_dir.glob("*.parquet"))
    if not existing:
        return None
    return pd.read_parquet(existing[0]).dtypes


def align_schema(frame: pd.DataFrame, schema: "pd.Series | None") -> pd.DataFrame:
    """Приводит месяц к схеме соседних файлов.

    При includeDesc=False часть колонок приходит пустой, и pandas выводит для
    них тип по содержимому — у одного месяца object, у другого float64. Сборка
    comtrade.db склеивает файлы через UNION ALL и на таком расхождении падает.
    """
    if schema is None:
        return frame
    aligned = frame.reindex(columns=list(schema.index))
    for column, dtype in schema.items():
        try:
            aligned[column] = aligned[column].astype(dtype)
        except (TypeError, ValueError):
            logger.debug("Колонка %s не приводится к %s, оставлена как есть", column, dtype)
    return aligned


def build_plan(
    api,
    *,
    output_dir: Path,
    periods: list[tuple[int, int]],
    manifest: dict,
    check_revisions: bool,
    reporter_ids: set[str],
) -> list[PeriodPlan]:
    """Что качать: недостающие месяцы плюс, при --refresh/--check, пересмотры."""
    plans = []
    for year, month in periods:
        plan = PeriodPlan(year=year, month=month)
        target = period_file(output_dir, year, month)
        if not target.exists():
            plan.missing = True
            plans.append(plan)
            continue
        if not check_revisions:
            continue
        # В доступности встречаются агрегаты вроде ЕС-27 (код 97); справочник
        # партнёров их уже отфильтровал, оставляем только страны.
        available = {
            code: meta
            for code, meta in availability_index(fetch_availability(api, plan.code)).items()
            if code in reporter_ids
        }
        if not available:
            continue
        mtime = datetime.fromtimestamp(target.stat().st_mtime)
        present = set(
            pd.read_parquet(target, columns=["reporterCode"])["reporterCode"].astype(str)
        )
        plan.new_reporters, plan.revised_reporters = stale_reporters(
            available, manifest.get(plan.name, {}).get("reporters"), mtime, present
        )
        plan.reporters = plan.new_reporters + plan.revised_reporters
        if plan.reporters:
            plans.append(plan)
    return plans


def collect(
    *,
    project_root: Path,
    start_year: int,
    refresh: bool,
    check_only: bool,
    dry_run: bool,
    only_periods: list[tuple[int, int]] | None = None,
    today: date | None = None,
) -> int:
    """Возвращает число записанных файлов."""
    import comtradeapicall as api

    output_dir = project_root / OUTPUT_SUBDIR
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = read_manifest(output_dir)

    # Справочник партнёров и справочник доступности отдаются без ключа, поэтому
    # план строится даже когда ключа нет или квота исчерпана.
    reporters = active_reporters(api)
    reporter_ids = [str(code) for code in reporters["id"]]
    logger.info("Действующих репортёров в справочнике: %d", len(reporter_ids))

    periods = enumerate_periods(start_year, last_closed_month(today or date.today()))
    if only_periods:
        wanted_periods = set(only_periods)
        skipped = wanted_periods - set(periods)
        if skipped:
            logger.warning(
                "Вне диапазона и пропущены: %s",
                ", ".join(f"{y}-{m:02d}" for y, m in sorted(skipped)),
            )
        periods = [p for p in periods if p in wanted_periods]
    plans = build_plan(
        api,
        output_dir=output_dir,
        periods=periods,
        manifest=manifest,
        check_revisions=refresh or check_only,
        reporter_ids=set(reporter_ids),
    )

    missing = [p for p in plans if p.missing]
    revisions = [p for p in plans if not p.missing]
    logger.info(
        "Недостающих месяцев: %d, месяцев с пересмотрами: %d (стран к перекачке: %d)",
        len(missing),
        len(revisions),
        sum(len(p.reporters) for p in revisions),
    )
    for plan in revisions:
        logger.info(
            "  %s: новых %d, пересмотрено %d",
            plan.name,
            len(plan.new_reporters),
            len(plan.revised_reporters),
        )
    if check_only or dry_run or not plans:
        for plan in missing:
            logger.info("  %s: файла нет", plan.name)
        return 0

    key = load_api_key(project_root)
    schema = reference_schema(output_dir)

    saved = 0
    for plan in plans:
        target = period_file(output_dir, plan.year, plan.month)
        available = availability_index(fetch_availability(api, plan.code))
        if not available:
            logger.info("Период %s: данные не предоставила ни одна страна", plan.name)
            continue

        candidates = reporter_ids if plan.missing else plan.reporters
        wanted = [code for code in candidates if code in available]
        logger.info("Период %s: запрашиваем %d стран", plan.name, len(wanted))

        frames: list[pd.DataFrame] = []
        try:
            for code in wanted:
                frames.extend(fetch_reporter(api, key, plan.code, code))
        except QuotaExceeded as exc:
            logger.error("%s. Запустите скрипт снова позже — он продолжит с этого места.", exc)
            write_manifest(output_dir, manifest)
            raise

        fresh = align_schema(pd.concat(frames, ignore_index=True), schema) if frames else pd.DataFrame()
        if plan.missing:
            if fresh.empty:
                logger.info("Период %s: торговли с Россией не обнаружено", plan.name)
                continue
            combined = fresh
        else:
            existing = pd.read_parquet(target)
            combined = align_schema(splice_reporters(existing, fresh, wanted), schema)

        combined.to_parquet(target, index=False)
        if schema is None:
            schema = combined.dtypes
        manifest[plan.name] = {
            "downloaded_at": datetime.now().isoformat(timespec="seconds"),
            "rows": int(len(combined)),
            "reporters": {code: available[code] for code in available if code in set(
                combined["reporterCode"].astype(str)
            )},
        }
        write_manifest(output_dir, manifest)
        logger.info("Сохранено %s: %d строк", target.name, len(combined))
        saved += 1
    return saved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start-year", type=valid_year, default=str(DEFAULT_START_YEAR))
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="помимо недостающих месяцев перекачать страны, пересмотревшие отчётность",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="только отчёт об устаревании: без ключа, без загрузки, без расхода квоты",
    )
    parser.add_argument(
        "--period",
        type=parse_period,
        action="append",
        metavar="YYYY-MM",
        help="ограничиться указанным месяцем; можно повторять. Полезно, чтобы "
        "разбить первый большой --refresh на части по квоте",
    )
    parser.add_argument("--dry-run", action="store_true", help="показать план без загрузки")
    args = parser.parse_args(argv)

    try:
        collect(
            project_root=get_project_root(),
            start_year=int(args.start_year),
            refresh=args.refresh,
            check_only=args.check,
            dry_run=args.dry_run,
            only_periods=args.period,
        )
    except QuotaExceeded:
        return QUOTA_EXIT_CODE
    return 0


if __name__ == "__main__":
    sys.exit(main())
