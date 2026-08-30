#!/usr/bin/env python3
"""Тесты сборщика данных Comtrade: план загрузки и учёт пересмотров."""

import argparse
from datetime import date, datetime

import pandas as pd
import pytest

from collectors import comtrade_collector
from collectors.comtrade_collector import (
    BudgetSpent,
    RequestBudget,
    align_schema,
    chunked,
    availability_index,
    enumerate_periods,
    last_closed_month,
    parse_period,
    period_file,
    splice_reporters,
    stale_reporters,
)


class TestPeriods:
    def test_current_month_is_not_downloaded(self):
        """За текущий месяц данные неполны, а файл потом не переписывается."""
        assert last_closed_month(date(2026, 8, 29)) == (2026, 7)

    def test_january_rolls_back_to_december(self):
        assert last_closed_month(date(2026, 1, 15)) == (2025, 12)

    def test_periods_stop_at_the_last_closed_month(self):
        periods = enumerate_periods(2025, (2026, 3))
        assert periods[0] == (2025, 1)
        assert periods[-1] == (2026, 3)
        assert len(periods) == 15

    def test_file_name_matches_what_the_merge_reads(self, tmp_path):
        assert period_file(tmp_path, 2026, 6).name == "2026-06.parquet"

    def test_period_argument_is_parsed(self):
        assert parse_period("2026-06") == (2026, 6)

    @pytest.mark.parametrize("value", ["2026-13", "2026", "июнь", "2026-00"])
    def test_bad_period_argument_is_rejected(self, value):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_period(value)


class TestRequestBudget:
    """На бесплатном тарифе прогон почти всегда упирается в лимит."""

    def test_budget_stops_at_the_limit(self):
        budget = RequestBudget(2)
        budget.take()
        budget.take()
        with pytest.raises(BudgetSpent):
            budget.take()
        assert budget.spent == 2

    def test_no_limit_never_stops(self):
        budget = RequestBudget(None)
        for _ in range(50):
            budget.take()
        assert budget.left == "без ограничения"

    def test_remaining_is_reported(self):
        budget = RequestBudget(10)
        budget.take()
        assert budget.left == "9"


class TestBatching:
    def test_reporters_are_split_into_batches(self):
        assert chunked(["1", "2", "3", "4", "5"], 2) == [["1", "2"], ["3", "4"], ["5"]]

    def test_batch_larger_than_the_list_is_one_request(self):
        assert chunked(["1", "2"], 8) == [["1", "2"]]

    def test_empty_list_needs_no_requests(self):
        assert chunked([], 8) == []


class TestStaleReporters:
    """Отбор стран к перекачке по справочнику доступности."""

    available = {
        "156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"},
        "398": {"checksum": "bbb", "lastReleased": "2026-08-02T10:00:00"},
    }

    def test_changed_checksum_marks_revision(self):
        recorded = {
            "156": {"checksum": "aaa", "lastReleased": "2026-01-01T00:00:00"},
            "398": {"checksum": "OLD", "lastReleased": "2026-01-01T00:00:00"},
        }
        new, revised = stale_reporters(self.available, recorded, None, {"156", "398"})
        assert new == []
        assert revised == ["398"]

    def test_reporter_absent_from_file_is_new(self):
        """2026-06 сохранён с одним репортёром — остальные должны догрузиться."""
        recorded = {"156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"}}
        new, revised = stale_reporters(self.available, recorded, None, {"156"})
        assert new == ["398"]
        assert revised == []

    def test_reporter_with_no_russia_trade_is_not_asked_again(self):
        """Страна отчиталась в Comtrade, но не торгует с Россией.

        Строк она не даёт и в parquet не попадает, зато остаётся в манифесте.
        Иначе её пришлось бы запрашивать в каждом прогоне — постоянная утечка
        суточной квоты.
        """
        recorded = {
            "156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"},
            "398": {"checksum": "bbb", "lastReleased": "2026-08-02T10:00:00"},
        }
        # present_in_file не передаётся: манифест есть, он и есть список опрошенных
        assert stale_reporters(self.available, recorded, None) == ([], [])

    def test_reporter_in_file_but_missing_from_manifest_is_judged_by_date(self):
        """Манифест может оказаться неполнее файла — это не повод качать заново.

        Так вышло после первого обновления месяца: скачали 37 стран и записали
        их, а ещё 45 были проверены, признаны свежими и остались в файле, но не
        в манифесте. Считать их новыми — 126 лишних запросов на ровном месте.
        """
        recorded = {"156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"}}
        mtime = datetime(2026, 8, 30, 12, 0, 0)  # файл переписан сегодня
        new, revised = stale_reporters(self.available, recorded, mtime, {"156", "398"})
        assert new == []
        assert revised == []

    def test_reporter_in_file_revised_after_the_file_is_refetched(self):
        recorded = {"156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"}}
        mtime = datetime(2026, 8, 1, 12, 0, 0)  # файл старше публикации 398
        new, revised = stale_reporters(self.available, recorded, mtime, {"156", "398"})
        assert (new, revised) == ([], ["398"])

    def test_unchanged_reporter_is_left_alone(self):
        recorded = {
            "156": {"checksum": "aaa", "lastReleased": "2026-08-01T10:00:00"},
            "398": {"checksum": "bbb", "lastReleased": "2026-08-02T10:00:00"},
        }
        assert stale_reporters(self.available, recorded, None, {"156", "398"}) == ([], [])

    def test_without_manifest_publication_date_decides(self):
        """Файлы, скачанные до появления манифеста, сверяются по времени файла."""
        mtime = datetime(2026, 8, 1, 12, 0, 0)
        new, revised = stale_reporters(self.available, None, mtime, {"156", "398"})
        assert new == []
        assert revised == ["398"]  # опубликован 2 августа, файл от 1 августа

    def test_nanoseconds_in_publication_date_do_not_break_comparison(self):
        available = {"156": {"checksum": "a", "lastReleased": "2026-08-03T19:51:21.7333333"}}
        new, revised = stale_reporters(available, None, datetime(2026, 8, 1), {"156"})
        assert revised == ["156"]

    def test_index_is_keyed_by_reporter_code_as_string(self):
        frame = pd.DataFrame(
            [{"reporterCode": 156, "datasetChecksum": -140332, "lastReleased": "2026-08-01"}]
        )
        assert availability_index(frame) == {
            "156": {"checksum": "-140332", "lastReleased": "2026-08-01"}
        }

    def test_empty_availability_yields_nothing(self):
        assert availability_index(pd.DataFrame()) == {}


class FakeApi:
    """Заглушка Comtrade: отдаёт заранее заданные ответы по очереди."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def getFinalData(self, key, **kwargs):
        self.calls.append(kwargs["reporterCode"])
        return self.responses.pop(0)


def rows(codes, n):
    return pd.DataFrame({"reporterCode": [c for c in codes for _ in range(n)]})


class TestFailedRequestsAreNotDataLoss:
    """Не-200 от API нельзя принимать за «страна не торгует с Россией».

    Библиотека на любой не-200 (обычно HTTP 429) возвращает None, а на честно
    пустой результат — пустой DataFrame. Если их не различать, замещение строк
    сотрёт уже сохранённые данные: так из файлов пропала Великобритания.
    """

    def test_none_response_marks_reporters_as_failed(self, monkeypatch):
        monkeypatch.setattr(comtrade_collector.time, "sleep", lambda *_: None)
        api = FakeApi([None])
        frames, failed = comtrade_collector.fetch_reporters(
            api, "k", "202001", ["826", "276"], RequestBudget(None)
        )
        assert frames == []
        assert failed == {"826", "276"}

    def test_empty_response_is_not_a_failure(self, monkeypatch):
        monkeypatch.setattr(comtrade_collector.time, "sleep", lambda *_: None)
        api = FakeApi([pd.DataFrame()])
        frames, failed = comtrade_collector.fetch_reporters(
            api, "k", "202001", ["500"], RequestBudget(None)
        )
        assert (frames, failed) == ([], set())

    def test_capped_response_is_split_until_it_fits(self, monkeypatch):
        """Сервер молча отдаёт первые 100 000 строк и роняет лишние страны."""
        monkeypatch.setattr(comtrade_collector.time, "sleep", lambda *_: None)
        capped = rows(["826", "276"], comtrade_collector.MAX_RECORDS // 2)
        api = FakeApi([capped, rows(["826"], 10), rows(["276"], 10)])
        frames, failed = comtrade_collector.fetch_reporters(
            api, "k", "202001", ["826", "276"], RequestBudget(None)
        )
        assert failed == set()
        assert len(api.calls) == 3
        assert pd.concat(frames)["reporterCode"].nunique() == 2

    def test_single_reporter_at_the_cap_is_split_by_flow(self, monkeypatch):
        monkeypatch.setattr(comtrade_collector.time, "sleep", lambda *_: None)
        api = FakeApi([rows(["276"], comtrade_collector.MAX_RECORDS),
                       rows(["276"], 5), rows(["276"], 5)])
        frames, failed = comtrade_collector.fetch_reporters(
            api, "k", "202001", ["276"], RequestBudget(None)
        )
        assert failed == set()
        assert [c for c in api.calls] == ["276", "276", "276"]

    def test_failure_inside_a_split_propagates(self, monkeypatch):
        monkeypatch.setattr(comtrade_collector.time, "sleep", lambda *_: None)
        capped = rows(["826", "276"], comtrade_collector.MAX_RECORDS // 2)
        api = FakeApi([capped, rows(["826"], 10), None])
        frames, failed = comtrade_collector.fetch_reporters(
            api, "k", "202001", ["826", "276"], RequestBudget(None)
        )
        assert failed == {"276"}


class TestSplice:
    def make(self, codes):
        return pd.DataFrame({"reporterCode": codes, "primaryValue": range(len(codes))})

    def test_only_named_reporters_are_replaced(self):
        existing = self.make([156, 156, 398, 792])
        fresh = self.make([156])
        result = splice_reporters(existing, fresh, ["156"])
        assert sorted(result["reporterCode"].tolist()) == [156, 398, 792]

    def test_reporter_with_no_fresh_rows_is_dropped(self):
        """Страна отозвала отчёт — её старые строки не должны остаться."""
        existing = self.make([156, 398])
        result = splice_reporters(existing, pd.DataFrame(), ["398"])
        assert result["reporterCode"].tolist() == [156]

    def test_untouched_reporters_keep_their_rows(self):
        existing = self.make([156, 398])
        result = splice_reporters(existing, self.make([156]), ["156"])
        assert (result[result["reporterCode"] == 398]["primaryValue"] == 1).all()

    def test_empty_existing_returns_fresh(self):
        fresh = self.make([156])
        assert splice_reporters(pd.DataFrame(), fresh, ["156"]).equals(fresh)


class TestSchemaAlignment:
    """Файлы склеиваются через UNION ALL — типы должны совпадать."""

    def test_empty_column_is_cast_to_the_neighbours_type(self):
        schema = pd.DataFrame({"cmdCode": ["01"], "cmdDesc": [None]}).astype(
            {"cmdCode": "object", "cmdDesc": "float64"}
        ).dtypes
        frame = pd.DataFrame({"cmdCode": ["0101"], "cmdDesc": [None]})
        assert align_schema(frame, schema).dtypes.equals(schema)

    def test_missing_column_is_added(self):
        schema = pd.DataFrame({"a": [1], "b": [2.0]}).dtypes
        aligned = align_schema(pd.DataFrame({"a": [1]}), schema)
        assert list(aligned.columns) == ["a", "b"]

    def test_column_order_follows_the_schema(self):
        schema = pd.DataFrame({"a": [1], "b": [2]}).dtypes
        aligned = align_schema(pd.DataFrame({"b": [2], "a": [1]}), schema)
        assert list(aligned.columns) == ["a", "b"]

    def test_no_schema_leaves_frame_untouched(self):
        frame = pd.DataFrame({"a": [1]})
        assert align_schema(frame, None) is frame


@pytest.mark.parametrize("value", ["", None])
def test_missing_publication_date_is_treated_as_stale(value):
    """Нет даты публикации — считаем данные подозрительными, а не свежими."""
    available = {"156": {"checksum": "a", "lastReleased": value or ""}}
    _, revised = stale_reporters(available, None, datetime(2026, 8, 1), {"156"})
    assert revised == ["156"]
