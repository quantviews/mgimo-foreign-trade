"""Повторный перевод подозрительных наименований ТН ВЭД.

Прежние переводы делались по одному названию, без глоссария и без указания
товарной позиции, к которой код относится. Отсюда "соли thereof", "другие" в
качестве самостоятельного наименования и обрывы посреди строки.

Здесь модели дают три вещи сразу: английский оригинал, текущий перевод и
официальное русское наименование ближайшего родителя из справочника ФТС/ФНС —
чтобы терминология совпадала с официальным текстом, а не изобреталась заново.
Ответ проверяется теми же признаками, по которым отбирались кандидаты
(scripts/find_suspicious_tnved_names.py); не прошедшие проверку остаются
с прежним переводом и попадают в отчёт.

Результат пишется в missing_codes_translations.json, прежнее значение
сохраняется в previous_russian_name. Происхождение наименования остаётся `mt`:
это по-прежнему машинный перевод, просто сделанный аккуратнее.

Запуск:
    python scripts/retranslate_tnved_names.py --limit 20 --dry-run
    python scripts/retranslate_tnved_names.py
Ключ читается из OPENAI_API_KEY или из .env в корне проекта.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from find_suspicious_tnved_names import signals, squash  # noqa: E402

TRANSLATIONS = PROJECT_ROOT / "metadata" / "translations" / "missing_codes_translations.json"
SUSPICIOUS = PROJECT_ROOT / "metadata" / "translations" / "suspicious_names.json"
REPORT = PROJECT_ROOT / "metadata" / "translations" / "retranslation_report.json"
DEFAULT_DB = PROJECT_ROOT / "db" / "unified_trade_data.duckdb"

API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_MODEL = "gpt-5.5"
BATCH_SIZE = 20

SYSTEM_PROMPT = """Ты редактируешь русские наименования товаров для справочника ТН ВЭД ЕАЭС.

На вход — позиции с полями: code (код), original (наименование в оригинале, обычно
по-английски), parent (официальное русское наименование ближайшей вышестоящей позиции
из справочника ФТС), current (текущий машинный перевод, часто неудачный).

Задача: дать корректное русское наименование, согласованное по терминологии с parent.

Правила:
- Стиль официального текста ТН ВЭД: именительный падеж, существительное впереди
  ("Ткани хлопчатобумажные прочие", а не "Другие хлопковые ткани").
- "other" — "прочие"; "not elsewhere specified/included", "n.e.s." —
  "в другом месте не поименованные или не включенные"; "excl." — "кроме";
  "incl." — "включая"; "% by weight" — "мас.%"; "knitted or crocheted" —
  "трикотажные машинного или ручного вязания"; "thereof" — "этих соединений"
  или "их" по смыслу.
- Наименование должно быть понятно само по себе. Если original — это просто
  "Other", разверни его, опираясь на parent: не "Прочие", а "Ткани хлопчатобумажные
  прочие". Никогда не отвечай одним служебным словом.
- Латинские названия видов, торговые марки, буквенные обозначения материалов и
  химические формулы оставляй как в оригинале.
- Числовые пороги, размеры, доли и единицы переноси точно, ничего не округляя.
- Не смешивай кириллицу и латиницу внутри слова. Не обрывай строку. Скобки и
  кавычки закрывай. Точку в конце не ставь.

Ответ — строго JSON: {"items": [{"code": "...", "name": "..."}]} для всех позиций входа."""


def load_api_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if key:
        return key
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "OPENAI_API_KEY":
                return value.strip().strip('"').strip("'")
    raise SystemExit("OPENAI_API_KEY не найден ни в окружении, ни в .env")


def load_official_names(db_path: Path) -> dict[str, str]:
    """Официальные наименования по кодам всех уровней (без машинных переводов)."""
    if not db_path.exists():
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            "SELECT TNVED_CODE, TNVED_NAME FROM tnved_reference WHERE NAME_SOURCE <> 'mt'"
        ).fetchall()
    finally:
        conn.close()
    return {code: name for code, name in rows}


def nearest_official(code: str, official: dict[str, str]) -> str:
    for length in (8, 6, 4, 2):
        name = official.get(code[:length])
        if name:
            return name
    return ""


def call_model(key: str, model: str, batch: list[dict]) -> tuple[dict[str, str], dict]:
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps({"items": batch}, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        payload = json.load(response)
    content = json.loads(payload["choices"][0]["message"]["content"])
    answers = {str(item["code"]): squash(item.get("name")) for item in content.get("items", [])}
    return answers, payload.get("usage", {})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--limit", type=int, help="обработать только N кодов")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--dry-run", action="store_true", help="не записывать результат")
    args = parser.parse_args()

    key = load_api_key()
    suspicious = json.loads(SUSPICIOUS.read_text(encoding="utf-8"))
    translations = json.loads(TRANSLATIONS.read_text(encoding="utf-8"))
    official = load_official_names(args.db)
    if not official:
        print(f"внимание: справочник {args.db} недоступен, контекст родителя будет пустым")

    # Доканчиваем прерванный прогон: уже переписанные коды пропускаем.
    todo = [
        code
        for code in suspicious
        if code in translations and "previous_russian_name" not in translations[code]
    ]
    if args.limit:
        todo = todo[: args.limit]
    total_batches = (len(todo) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"к переводу: {len(todo)} из {len(suspicious)} подозрительных")

    accepted: dict[str, str] = {}
    rejected: dict[str, dict] = {}
    tokens_in = tokens_out = 0
    for start in range(0, len(todo), BATCH_SIZE):
        chunk = todo[start : start + BATCH_SIZE]
        batch = [
            {
                "code": code,
                "original": suspicious[code]["original_name"],
                "parent": nearest_official(code, official),
                "current": suspicious[code]["russian_name"],
            }
            for code in chunk
        ]
        try:
            answers, usage = call_model(key, args.model, batch)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:200]
            print(f"  батч {start // BATCH_SIZE + 1}: HTTP {exc.code} {detail}")
            continue
        tokens_in += usage.get("prompt_tokens", 0)
        tokens_out += usage.get("completion_tokens", 0)

        for code in chunk:
            name = answers.get(code, "")
            problems = (
                signals(name, suspicious[code]["original_name"], Counter())
                if name
                else ["empty"]
            )
            # duplicate не блокирует: счётчик повторов строится по всему
            # справочнику, внутри одного батча его не собрать.
            blocking = [p for p in problems if p != "duplicate"]
            if name and not blocking:
                accepted[code] = name
            else:
                rejected[code] = {"name": name, "problems": problems}
        print(
            f"  батч {start // BATCH_SIZE + 1}/{total_batches}: "
            f"принято {len(accepted)}, отклонено {len(rejected)}"
        )

    print(f"токенов: {tokens_in} вход, {tokens_out} выход")
    if args.dry_run:
        for code, name in list(accepted.items())[:12]:
            print(f"  {code}  {suspicious[code]['russian_name'][:45]}  ->  {name[:55]}")
        return

    for code, name in accepted.items():
        entry = translations[code]
        entry["previous_russian_name"] = entry.get("russian_name", "")
        entry["russian_name"] = name
    TRANSLATIONS.write_text(
        json.dumps(translations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    REPORT.write_text(
        json.dumps(
            {"model": args.model, "accepted": len(accepted), "rejected": rejected},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"записано: {len(accepted)}, отклонено {len(rejected)} (см. {REPORT.name})")


if __name__ == "__main__":
    main()
