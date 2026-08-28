"""Отбор машинных наименований ТН ВЭД, которые стоит перевести заново.

Переводы в metadata/translations/missing_codes_translations.json сделаны
gpt-4o-mini по одному названию, без глоссария и без контекста родительской
позиции. Скрипт отбирает те, где это видно по формальным признакам, чтобы не
гонять через модель весь справочник.

Признаки (код может попасть по нескольким):
  mixed_script      кириллица вплотную к латинице внутри слова
  lost_symbol       "?" вместо утерянного символа валюты или градуса
  truncated         строка обрывается на запятой, скобке или предлоге
  unbalanced        непарные скобки или кавычки
  english_leftover  непереведённое английское слово вне кавычек и скобок
  uninformative     название состоит только из служебных слов ("другие")
  duplicate         одно и то же название у восьми и более разных кодов
  untranslated      совпадает с оригиналом

Запуск: python scripts/find_suspicious_tnved_names.py
Результат: metadata/translations/suspicious_names.json
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRANSLATIONS = PROJECT_ROOT / "metadata" / "translations" / "missing_codes_translations.json"
OVERRIDES = PROJECT_ROOT / "metadata" / "translations" / "name_overrides.json"
OUTPUT = PROJECT_ROOT / "metadata" / "translations" / "suspicious_names.json"

# Английские слова, которые переводчик оставил как есть. Латинские названия
# видов (Gallus domesticus, Vigna radiata) сюда не входят намеренно — в тексте
# ТН ВЭД они на месте.
ENGLISH_LEFTOVERS = frozenset({
    "thereof", "other", "others", "whether", "excl", "incl", "not", "elsewhere",
    "specified", "board", "ware", "crochet", "crocheted", "knitted", "the",
    "and", "with", "for", "from", "nes", "than", "more", "less", "over",
    "under", "parts", "used", "made", "type", "rayon",
})

# Названия, не несущие информации без родительской позиции.
UNINFORMATIVE = frozenset({
    "другие", "другое", "другая", "другой", "прочие", "прочее", "прочая",
    "прочий", "серый", "остальные", "неопределенные",
})

MIXED_SCRIPT = re.compile(r"[А-Яа-яЁё][A-Za-z]|[A-Za-z][А-Яа-яЁё]")
TRUNCATED = re.compile(r"[,(]$|\b(из|для|или|и|с|в|на|по)$")
LOWER_LATIN = re.compile(r"\b[a-z]{2,}\b")
CYRILLIC_WORD = re.compile(r"[а-яё]+")
QUOTED_OR_BRACKETED = re.compile(r'"[^"]*"|\([^)]*\)')

DUPLICATE_THRESHOLD = 8

# Расхождение длины с оригиналом признаком не является. В первом заходе оно
# дало 185 срабатываний и помогло собрать сеть пошире, но после переработки
# наименований — 559, из них 541 на корректных строках: официальная формулировка
# ТН ВЭД ("в другом месте не поименованная или не включенная") втрое длиннее
# английского "n.e.s.", а короче оригинала оказываются те коды, где неверен сам
# английский исходник. Настоящие обрывы ловят truncated, unbalanced и
# uninformative.


def squash(text: str | None) -> str:
    return " ".join((text or "").split())


def signals(russian: str, original: str, duplicates: Counter) -> list[str]:
    found = []
    lowered = russian.lower()
    if MIXED_SCRIPT.search(russian):
        found.append("mixed_script")
    if "?" in russian:
        found.append("lost_symbol")
    if TRUNCATED.search(russian):
        found.append("truncated")
    if russian.count("(") != russian.count(")") or russian.count('"') % 2:
        found.append("unbalanced")
    outside = QUOTED_OR_BRACKETED.sub("", russian)
    if set(LOWER_LATIN.findall(outside)) & ENGLISH_LEFTOVERS:
        found.append("english_leftover")
    words = set(CYRILLIC_WORD.findall(lowered))
    if words and words <= UNINFORMATIVE:
        found.append("uninformative")
    elif duplicates[lowered] >= DUPLICATE_THRESHOLD:
        found.append("duplicate")
    if original and lowered == original.lower():
        found.append("untranslated")
    return found


def main() -> None:
    translations = json.loads(TRANSLATIONS.read_text(encoding="utf-8"))
    overrides = (
        json.loads(OVERRIDES.read_text(encoding="utf-8")) if OVERRIDES.exists() else {}
    )
    # Выверенные вручную наименования переводить заново незачем.
    candidates = {k: v for k, v in translations.items() if k not in overrides}

    duplicates = Counter(squash(v.get("russian_name")).lower() for v in candidates.values())

    suspicious = {}
    for code, data in sorted(candidates.items()):
        russian = squash(data.get("russian_name"))
        original = squash(data.get("original_name"))
        found = signals(russian, original, duplicates)
        if found:
            suspicious[code] = {
                "russian_name": russian,
                "original_name": original,
                "signals": found,
            }

    OUTPUT.write_text(
        json.dumps(suspicious, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    counts = Counter(s for v in suspicious.values() for s in v["signals"])
    print(f"кандидатов: {len(candidates)}, подозрительных: {len(suspicious)}")
    for signal, count in counts.most_common():
        print(f"  {count:5d}  {signal}")
    print(f"записано: {OUTPUT}")


if __name__ == "__main__":
    main()
