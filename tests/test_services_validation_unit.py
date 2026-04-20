"""Unit tests for etl_service.validation module."""

import pandas as pd

from etl_service.validation import (
    SeverityLevel,
    ValidationResult,
    _validate_hour_totals,
    _validate_required_fields,
    _validate_row_hours,
    format_validation_report,
    validate_plan_data,
)


def test_validate_row_hours_reports_negative_and_non_numeric():
    row = pd.Series(["Тема", -1, None, "bad", 2, 1])
    issues = _validate_row_hours(row, row_num=10)

    issue_types = {i.issue_type for i in issues}
    assert "negative_hours" in issue_types
    assert "invalid_number" in issue_types
    assert all(i.severity == SeverityLevel.ERROR for i in issues)


def test_validate_required_fields_reports_whitespace_label():
    row = pd.Series(["   ", 1, None, 0, 0, 0])
    issues = _validate_required_fields(row, row_num=8, label="   ")

    assert len(issues) == 1
    assert issues[0].issue_type == "empty_field"


def test_validate_hour_totals_warns_on_mismatch():
    row = pd.Series(["Тема", 10, None, 2, 3, 1])
    issues = _validate_hour_totals(row, row_num=12)

    assert len(issues) == 1
    assert issues[0].severity == SeverityLevel.WARNING
    assert issues[0].issue_type == "hour_mismatch"


def test_validate_hour_totals_ok_on_match():
    row = pd.Series(["Тема", 6, None, 2, 3, 1])
    issues = _validate_hour_totals(row, row_num=13)
    assert issues == []


def test_validate_plan_data_collects_errors_and_warnings():
    # 4 header rows + content rows
    df = pd.DataFrame(
        [
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["Тема 1", 6, None, 2, 3, 1],
            ["Тема 2", 10, None, 2, 3, 1],
            ["Тема 3", -2, None, 0, 0, 0],
            [None, None, None, None, None, None],
        ]
    )

    result = validate_plan_data(df, skip_header_rows=4)

    assert isinstance(result, ValidationResult)
    assert result.error_count >= 1
    assert result.warning_count >= 1
    assert result.is_valid is False


def test_validate_plan_data_valid_dataset_returns_valid_true():
    df = pd.DataFrame(
        [
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["Тема 1", 5, None, 2, 2, 1],
            ["Тема 2", 4, None, 1, 1, 2],
        ]
    )

    result = validate_plan_data(df, skip_header_rows=4)

    assert result.is_valid is True
    assert result.error_count == 0


def test_format_validation_report_for_valid_and_invalid_results():
    valid_result = ValidationResult(True, [], [], 0, 0)
    valid_report = format_validation_report(valid_result)
    assert "VALIDATION REPORT" in valid_report
    assert "VALID" in valid_report

    invalid_df = pd.DataFrame(
        [
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["h", None, None, None, None, None],
            ["Тема X", 10, None, 1, 1, 1],
            ["Тема Y", -1, None, 0, 0, 0],
        ]
    )
    invalid_result = validate_plan_data(invalid_df, skip_header_rows=4)
    invalid_report = format_validation_report(invalid_result)

    assert "INVALID" in invalid_report
    assert "ERRORS" in invalid_report
    assert "WARNINGS" in invalid_report
