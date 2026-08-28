#!/usr/bin/env python3
"""Тесты отбора машинных наименований ТН ВЭД, требующих повторного перевода."""

import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from find_suspicious_tnved_names import signals, squash  # noqa: E402


def check(russian, original="Some reasonably long English product description", duplicates=None):
    return signals(russian, original, duplicates if duplicates is not None else Counter())


@pytest.mark.parametrize(
    "russian, expected",
    [
        ("ПОТASSIUM МЕТАБИСУЛЬФИТ", "mixed_script"),
        ("Сок стоимостью > 18 ? за 100 кг", "lost_symbol"),
        ("Изделия из меди, прочие, изготовленные из", "truncated"),
        ("Ткани хлопчатобумажные (кроме отбеленных", "unbalanced"),
        ("Хлорамфеникол и его производные; соли thereof", "english_leftover"),
        ("Другие", "uninformative"),
    ],
)
def test_signal_is_raised(russian, expected):
    assert expected in check(russian)


def test_clean_name_raises_nothing():
    assert check("Туши и полутуши свиней свежие или охлажденные") == []


def test_correct_wording_is_not_flagged_for_length():
    """Официальная формулировка втрое длиннее английской и это нормально."""
    assert check(
        "Рыба мороженая прочая, в другом месте не поименованная или не включенная",
        original="Frozen fish, n.e.s.",
    ) == []


def test_latin_species_name_is_left_alone():
    """Латинские названия видов в тексте ТН ВЭД на месте, это не недоперевод."""
    assert check("Тунец длинноперый (Thunnus alalunga), свежий или охлажденный") == []


def test_name_shared_by_many_codes_is_flagged():
    name = "мясо; свиней, не включенных в другие категории"
    assert "duplicate" in check("Мясо; свиней, не включенных в другие категории",
                                duplicates=Counter({name: 11}))
    assert "duplicate" not in check("Мясо; свиней, не включенных в другие категории",
                                    duplicates=Counter({name: 2}))


def test_untranslated_name_is_flagged():
    assert "untranslated" in check("Frozen fish", original="Frozen fish")


def test_squash_collapses_whitespace():
    assert squash("  Нефть   сырая\n и\tпрочее ") == "Нефть сырая и прочее"
    assert squash(None) == ""
