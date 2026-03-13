"""Tests for data_sourcing.py — ported from DataSourcingTests.cs (18 tests)."""

from datetime import date

import pytest

from etl.modules.data_sourcing import DataSourcing, ETL_EFFECTIVE_DATE_KEY


def make_module(
    min_date=None, max_date=None, lookback_days=None,
    most_recent_prior=False, most_recent=False,
):
    return DataSourcing(
        result_name="test_result",
        schema="datalake",
        table="test_table",
        columns=["id", "name"],
        min_effective_date=min_date,
        max_effective_date=max_date,
        lookback_days=lookback_days,
        most_recent_prior=most_recent_prior,
        most_recent=most_recent,
    )


# --- Validation: mutually exclusive modes ---


def test_constructor_lookback_and_static_dates_throws():
    with pytest.raises(ValueError):
        make_module(min_date=date(2024, 1, 1), lookback_days=3)


def test_constructor_most_recent_prior_and_static_dates_throws():
    with pytest.raises(ValueError):
        make_module(max_date=date(2024, 1, 31), most_recent_prior=True)


def test_constructor_lookback_and_most_recent_prior_throws():
    with pytest.raises(ValueError):
        make_module(lookback_days=3, most_recent_prior=True)


def test_constructor_negative_lookback_days_throws():
    with pytest.raises(ValueError):
        make_module(lookback_days=-1)


def test_constructor_lookback_only_does_not_throw():
    module = make_module(lookback_days=5)
    assert module is not None


def test_constructor_most_recent_prior_only_does_not_throw():
    module = make_module(most_recent_prior=True)
    assert module is not None


# --- ResolveDateRange: lookback mode ---


def test_resolve_date_range_lookback_returns_correct_range():
    module = make_module(lookback_days=3)
    state = {ETL_EFFECTIVE_DATE_KEY: date(2024, 10, 15)}
    result = module.resolve_date_range(state)
    assert result == (date(2024, 10, 12), date(2024, 10, 15))


def test_resolve_date_range_lookback_zero_min_equals_max():
    module = make_module(lookback_days=0)
    state = {ETL_EFFECTIVE_DATE_KEY: date(2024, 10, 15)}
    result = module.resolve_date_range(state)
    assert result == (date(2024, 10, 15), date(2024, 10, 15))


# --- ResolveDateRange: default / fallback ---


def test_resolve_date_range_no_modes_falls_back_to_etl_effective_date():
    module = make_module()
    state = {ETL_EFFECTIVE_DATE_KEY: date(2024, 10, 15)}
    result = module.resolve_date_range(state)
    assert result == (date(2024, 10, 15), date(2024, 10, 15))


def test_resolve_date_range_static_dates_uses_static_dates():
    module = make_module(min_date=date(2024, 1, 1), max_date=date(2024, 1, 31))
    state = {ETL_EFFECTIVE_DATE_KEY: date(2024, 10, 15)}
    result = module.resolve_date_range(state)
    assert result == (date(2024, 1, 1), date(2024, 1, 31))


def test_resolve_date_range_missing_etl_date_throws():
    module = make_module()
    state = {}
    with pytest.raises(RuntimeError):
        module.resolve_date_range(state)


def test_resolve_date_range_lookback_missing_etl_date_throws():
    module = make_module(lookback_days=3)
    state = {}
    with pytest.raises(RuntimeError):
        module.resolve_date_range(state)


def test_resolve_date_range_most_recent_prior_missing_etl_date_throws():
    module = make_module(most_recent_prior=True)
    state = {}
    with pytest.raises(RuntimeError):
        module.resolve_date_range(state)


# --- mostRecent mode ---


def test_constructor_most_recent_only_does_not_throw():
    module = make_module(most_recent=True)
    assert module is not None


def test_constructor_most_recent_and_static_dates_throws():
    with pytest.raises(ValueError):
        make_module(min_date=date(2024, 1, 1), most_recent=True)


def test_constructor_most_recent_and_lookback_throws():
    with pytest.raises(ValueError):
        make_module(lookback_days=3, most_recent=True)


def test_constructor_most_recent_and_most_recent_prior_throws():
    with pytest.raises(ValueError):
        make_module(most_recent_prior=True, most_recent=True)


def test_resolve_date_range_most_recent_missing_etl_date_throws():
    module = make_module(most_recent=True)
    state = {}
    with pytest.raises(RuntimeError):
        module.resolve_date_range(state)
