import asyncio
from app import analytical_writer as writer

FACTS = {
    "kz": {"kind": "total", "text": "Казахстан; окно 2025-09 - 2026-08. Стоимость в доступных записях (неполный итог): 139,84 тыс. долл."},
    "tr": {"kind": "total", "text": "Турция; окно 2025-09 - 2026-08. Стоимость в доступных записях (неполный итог): 7,36 тыс. долл."},
}


def test_numbers_must_belong_to_cited_evidence():
    assert writer.validate_blocks([{"text": "Казахстан: 139,84 тыс. долл.", "evidence": ["kz"]}], FACTS)
    assert not writer.validate_blocks([{"text": "Турция: 139,84 тыс. долл.", "evidence": ["tr"]}], FACTS)
    assert not writer.validate_blocks([{"text": "Итого 147,20 тыс. долл.", "evidence": ["kz", "tr"]}], FACTS)
    assert not writer.validate_blocks([{"text": "1 млн долл.", "evidence": ["unknown"]}], FACTS)
    assert not writer.validate_blocks([{"text": "На них приходится треть экспорта.", "evidence": ["kz", "tr"]}], FACTS)
    assert writer.numbers("116,00") == writer.numbers("116")


def test_semantic_gate_rejects_wrong_association_even_with_legal_number(monkeypatch):
    replies = iter([
        {"blocks": [{"text": "Турция: 139,84 тыс. долл.", "evidence": ["kz"]}]},
        {"valid": False, "errors": ["Wrong country"]},
        {"blocks": [{"text": "Турция: 139,84 тыс. долл.", "evidence": ["kz"]}]},
        {"valid": False, "errors": ["Wrong country"]},
    ])
    async def fake(*args):
        return next(replies)
    monkeypatch.setattr(writer, "json_completion", fake)
    assert asyncio.run(writer.write_answer("Экспорт водорода", FACTS)) is None


def test_prose_is_kept_after_both_gates(monkeypatch):
    replies = iter([
        {"blocks": [{"text": "Основное представленное направление - Казахстан: 139,84 тыс. долл. Данные неполные.",
                     "evidence": ["kz", "tr"]}]},
        {"valid": True, "errors": []},
    ])
    async def fake(*args):
        return next(replies)
    monkeypatch.setattr(writer, "json_completion", fake)
    answer = asyncio.run(writer.write_answer("Экспорт водорода", FACTS))
    assert answer.startswith("Основное представленное направление")
    assert "139,84" in answer


def test_fallback_has_one_table_and_one_partial_note():
    answer = writer.compact_fallback(FACTS)
    assert answer.count("| Страна") == 1
    assert answer.count("полный итог") == 1
    assert "139,84" in answer and "7,36" in answer
    assert "Отсутствуют месяцы" not in answer


def test_editor_sees_requested_metrics_and_fact_scope():
    facts = {**FACTS,
             "pred": {"kind": "total", "text": "Казахстан, оценка проекта (nowcast): 20,55 тыс. долл."},
             "mass": {"kind": "total", "text": "Казахстан. Масса по имеющимся записям: 38,07 т."},
             "yoy": {"kind": "limitation", "text": "Стоимость: динамика итогов год к году не рассчитана."}}
    assert set(writer.editorial_evidence(facts, "Экспорт водорода")) == set(FACTS) | {'mass'}
    assert set(writer.editorial_evidence(facts, "Масса, nowcast и динамика")) == set(facts)


def test_direction_and_invented_incompleteness_are_blocked_without_model():
    facts={"a":{"kind":"total","text":"Китай, импорт России. Стоимость по имеющимся записям: 100 долл."}}
    assert not writer.validate_blocks([{"text":"Импорт Китаем: 100 долл.","evidence":["a"]}],facts)
    assert not writer.validate_blocks([{"text":"Охват неполный: 100 долл.","evidence":["a"]}],facts)
    assert writer.validate_blocks([{"text":"Импорт России из Китая: 100 долл.","evidence":["a"]}],facts)


def test_how_many_cars_preserves_quantity():
    facts={"a":{"kind":"total","text":"Количество: 116,00 ШТУКА."}}
    assert writer.editorial_evidence(facts,"Сколько автомобилей импортировала Россия?")==facts
    assert not writer.validate_blocks([{"text":"Есть данные.","evidence":["a"]}],facts,
                                      "Сколько автомобилей импортировала Россия?")


def test_country_fallback_orders_different_units_without_summing():
    facts = {
        "a": {"kind": "aggregate", "text": "Турция, экспорт России, код 280410, статистика источника; фильтр 2025-09-01 - 2026-08-01: стоимость 7,36 тыс. долл., масса 145 кг. Покрытие месяцев не установлено."},
        "b": {"kind": "aggregate", "text": "Казахстан, экспорт России, код 280410, статистика источника; фильтр 2025-09-01 - 2026-08-01: стоимость 139,84 тыс. долл., масса 38,07 т. Покрытие месяцев не установлено."},
        "c": {"kind": "aggregate", "text": "Азербайджан, экспорт России, код 280410, статистика источника; фильтр 2025-09-01 - 2026-08-01: стоимость 367,15 долл., масса 10,9 кг. Покрытие месяцев не установлено."},
    }
    answer = writer.compact_fallback(facts)
    assert answer.index("| Казахстан") < answer.index("| Турция") < answer.index("| Азербайджан")
    assert "38,07" not in answer and "367,15 долл." in answer
