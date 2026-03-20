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
        assert list(result.columns) == ["day_of_week", "collision_count"]

    def test_counts_correctly(self):
        df = pd.DataFrame({"OCC_DOW": ["Monday", "Monday", "Monday", "Friday", "Friday"]})
        result = collisions_by_day_of_week(df)
        assert result.iloc[0]["day_of_week"] == "Monday"
        assert int(result.iloc[0]["collision_count"]) == 3

    def test_ignores_missing_values(self):
        df = pd.DataFrame({"OCC_DOW": ["Monday", None, "Friday", None, "Friday"]})
        result = collisions_by_day_of_week(df)
        assert result["day_of_week"].isna().sum() == 0
        assert int(result["collision_count"].sum()) == 3

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame({"OCC_DOW": pd.Series([], dtype="object")})
        result = collisions_by_day_of_week(df)
        assert len(result) == 0
        assert list(result.columns) == ["day_of_week", "collision_count"]

    def test_all_missing_returns_empty(self):
        df = pd.DataFrame({"OCC_DOW": [None, None, None]})
        result = collisions_by_day_of_week(df)
        assert len(result) == 0
        assert list(result.columns) == ["day_of_week", "collision_count"]

    def test_single_row(self):
        df = pd.DataFrame({"OCC_DOW": ["Wednesday"]})
        result = collisions_by_day_of_week(df)
        assert len(result) == 1
        assert result.iloc[0]["day_of_week"] == "Wednesday"
        assert int(result.iloc[0]["collision_count"]) == 1

    def test_missing_column_returns_empty_dataframe(self):
        df = pd.DataFrame({"WRONG_COLUMN": ["Monday", "Tuesday"]})
        result = collisions_by_day_of_week(df)
        assert result.empty
        assert list(result.columns) == ["day_of_week", "collision_count"]

    def test_all_same_day(self):
        df = pd.DataFrame({"OCC_DOW": ["Saturday"] * 10})
        result = collisions_by_day_of_week(df)
        assert len(result) == 1
        assert result.iloc[0]["day_of_week"] == "Saturday"
        assert int(result.iloc[0]["collision_count"]) == 10

    def test_sorted_by_day_order(self):
        df = pd.DataFrame({"OCC_DOW": ["Friday", "Monday", "Sunday", "Tuesday"]})
        result = collisions_by_day_of_week(df)
        assert result["day_of_week"].tolist() == ["Monday", "Tuesday", "Friday", "Sunday"]


class TestCollisionsByMonth:

    def test_returns_correct_columns(self):
        df = pd.DataFrame({"MONTH": [1, 2, 1]})
        result = collisions_by_month(df)
        assert list(result.columns) == ["month", "collision_count", "month_name"]

    def test_counts_correctly(self):
        df = pd.DataFrame({"MONTH": [1, 1, 1, 3, 3]})
        result = collisions_by_month(df)
        assert result.iloc[0]["month"] == 1
        assert result.iloc[0]["month_name"] == "January"
        assert int(result.iloc[0]["collision_count"]) == 3

    def test_ignores_missing_values(self):
        df = pd.DataFrame({"MONTH": [1, None, 3, None]})
        result = collisions_by_month(df)
        assert result["month"].isna().sum() == 0
        assert int(result["collision_count"].sum()) == 2

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame({"MONTH": pd.Series([], dtype="float")})
        result = collisions_by_month(df)
        assert len(result) == 0
        assert list(result.columns) == ["month_name", "collision_count"]

    def test_missing_column_returns_empty_dataframe(self):
        df = pd.DataFrame({"WRONG": [1]})
        result = collisions_by_month(df)
        assert result.empty
        assert list(result.columns) == ["month_name", "collision_count"]

    def test_all_missing_returns_empty(self):
        df = pd.DataFrame({"MONTH": [None, None]})
        result = collisions_by_month(df)
        assert len(result) == 0
        assert list(result.columns) == ["month_name", "collision_count"]

    def test_single_row(self):
        df = pd.DataFrame({"MONTH": [12]})
        result = collisions_by_month(df)
        assert len(result) == 1
        assert result.iloc[0]["month"] == 12
        assert result.iloc[0]["month_name"] == "December"
        assert int(result.iloc[0]["collision_count"]) == 1

    def test_orders_months_correctly(self):
        df = pd.DataFrame({"MONTH": [12, 1, 3, 2]})
        result = collisions_by_month(df)
        assert result["month_name"].tolist() == ["January", "February", "March", "December"]