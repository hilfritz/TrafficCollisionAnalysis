"""
Tests for day-of-week and monthly collision analysis.

TDD approach:
- RED: tests committed before functions were implemented
- GREEN: functions added to analytics.py to make tests pass
- REFACTOR: extracted shared grouping logic into helper
"""

import pandas as pd
import pytest

from src.analytics import collisions_by_day_of_week, collisions_by_month


class TestCollisionsByDayOfWeek:

    def test_returns_correct_columns(self):
        df = pd.DataFrame({"OCC_DOW": ["Monday", "Tuesday", "Monday"]})
        result = collisions_by_day_of_week(df)
        assert list(result.columns) == ["OCC_DOW", "collision_count"]

    def test_counts_correctly(self):
        df = pd.DataFrame({"OCC_DOW": ["Monday", "Monday", "Monday", "Friday", "Friday"]})
        result = collisions_by_day_of_week(df)
        assert result.iloc[0]["OCC_DOW"] == "Monday"
        assert int(result.iloc[0]["collision_count"]) == 3

    def test_ignores_missing_values(self):
        df = pd.DataFrame({"OCC_DOW": ["Monday", None, "Friday", None, "Friday"]})
        result = collisions_by_day_of_week(df)
        assert result["OCC_DOW"].isna().sum() == 0
        assert int(result["collision_count"].sum()) == 3

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame({"OCC_DOW": pd.Series([], dtype="object")})
        result = collisions_by_day_of_week(df)
        assert len(result) == 0

    def test_all_missing_returns_empty(self):
        df = pd.DataFrame({"OCC_DOW": [None, None, None]})
        result = collisions_by_day_of_week(df)
        assert len(result) == 0

    def test_single_row(self):
        df = pd.DataFrame({"OCC_DOW": ["Wednesday"]})
        result = collisions_by_day_of_week(df)
        assert len(result) == 1

    def test_raises_error_if_column_missing(self):
        df = pd.DataFrame({"WRONG_COLUMN": ["Monday", "Tuesday"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            collisions_by_day_of_week(df)

    def test_all_same_day(self):
        df = pd.DataFrame({"OCC_DOW": ["Saturday"] * 10})
        result = collisions_by_day_of_week(df)
        assert len(result) == 1
        assert int(result.iloc[0]["collision_count"]) == 10

    def test_sorted_by_count_descending(self):
        df = pd.DataFrame({"OCC_DOW": ["Mon", "Mon", "Mon", "Tue", "Tue", "Wed"]})
        result = collisions_by_day_of_week(df)
        counts = result["collision_count"].tolist()
        assert counts == sorted(counts, reverse=True)


class TestCollisionsByMonth:

    def test_returns_correct_columns(self):
        df = pd.DataFrame({"OCC_MONTH": ["January", "February", "January"]})
        result = collisions_by_month(df)
        assert list(result.columns) == ["OCC_MONTH", "collision_count"]

    def test_counts_correctly(self):
        df = pd.DataFrame({"OCC_MONTH": ["January", "January", "January", "March", "March"]})
        result = collisions_by_month(df)
        assert result.iloc[0]["OCC_MONTH"] == "January"
        assert int(result.iloc[0]["collision_count"]) == 3

    def test_ignores_missing_values(self):
        df = pd.DataFrame({"OCC_MONTH": ["January", None, "March", None]})
        result = collisions_by_month(df)
        assert result["OCC_MONTH"].isna().sum() == 0
        assert int(result["collision_count"].sum()) == 2

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame({"OCC_MONTH": pd.Series([], dtype="object")})
        result = collisions_by_month(df)
        assert len(result) == 0

    def test_raises_error_if_column_missing(self):
        df = pd.DataFrame({"WRONG": ["Jan"]})
        with pytest.raises(ValueError, match="Missing required columns"):
            collisions_by_month(df)

    def test_all_missing_returns_empty(self):
        df = pd.DataFrame({"OCC_MONTH": [None, None]})
        result = collisions_by_month(df)
        assert len(result) == 0

    def test_single_row(self):
        df = pd.DataFrame({"OCC_MONTH": ["December"]})
        result = collisions_by_month(df)
        assert len(result) == 1
