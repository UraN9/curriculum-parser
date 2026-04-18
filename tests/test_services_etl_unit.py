"""Unit tests for etl_service.etl and validation helpers."""

from types import SimpleNamespace

import pandas as pd
import pytest
from openpyxl import Workbook

from etl_service import etl
from etl_service.validation import ValidationIssue, ValidationResult, SeverityLevel


class FakeQuery:
    def filter_by(self, **_kwargs):
        return self

    def first(self):
        return None


class FakeDbSession:
    def query(self, _model):
        return FakeQuery()

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


def test_create_empty_theme_shape():
    theme = etl._create_empty_theme()
    assert theme["total"] == 0
    assert theme["lectures"] == 0
    assert theme["practical"] == 0
    assert theme["lab"] == 0
    assert theme["self"] == 0
    assert theme["activities"] == []


def test_extract_and_aggregate_data_with_minimal_rows(monkeypatch):
    df = pd.DataFrame(
        [
            ["hdr", None, None, None, None, None, None],
            ["hdr", None, None, None, None, None, None],
            ["hdr", None, None, None, None, None, None],
            ["hdr", None, None, None, None, None, None],
            ["5 СЕМЕСТР", None, None, None, None, None, None],
            ["РОЗДІЛ 1. TEST", None, None, None, None, None, None],
            ["Тема 1.1 Intro", None, None, None, None, None, None],
            ["Лекція 1", 2, None, 2, 0, 0, "опитування"],
            ["Практична 1", 2, None, 0, 2, 0, "захист"],
            ["Лабораторна 1", 2, None, 0, 2, 0, "конспект"],
            ["Самостійна 1", 2, None, 0, 0, 2, None],
        ]
    )

    monkeypatch.setattr(etl.pd, "read_excel", lambda *args, **kwargs: df)

    sections, themes, totals, semester = etl._extract_and_aggregate_data("dummy.xlsx")

    assert semester == 5
    assert sections == ["РОЗДІЛ 1. TEST"]
    assert len(themes) == 1
    assert totals["total"] == 8
    assert totals["lectures"] == 2
    assert totals["practical"] == 2
    assert totals["lab"] == 2
    assert totals["self"] == 2


def test_build_structure_table_contains_grand_total():
    sections = ["РОЗДІЛ 1. TEST"]
    themes = {
        ("РОЗДІЛ 1. TEST", "Тема 1.1 Intro"): {
            "section": "РОЗДІЛ 1. TEST",
            "theme": "Тема 1.1 Intro",
            "total": 10,
            "lectures": 2,
            "practical": 3,
            "lab": 1,
            "individual": 0,
            "self": 4,
            "activities": [],
        }
    }
    totals = {"total": 10, "lectures": 2, "practical": 3, "lab": 1, "individual": 0, "self": 4}

    table = etl._build_structure_table(sections, themes, totals)

    assert table[0][0] == "Назви змістових модулів і тем"
    assert any(str(row[0]).startswith("Разом за розділом") for row in table)
    assert table[-1][0] == "ВСЬОГО ПО НАВЧАЛЬНІЙ ДИСЦИПЛІНІ:"


def test_worksheet_helpers_apply_merges_and_styles():
    wb = Workbook()
    ws = wb.active

    structure_data = [
        ["Назви змістових модулів і тем", "Кількість годин", "", "", "", "", ""],
        ["", "денна форма", "", "", "", "", ""],
        ["", "усього", "у тому числі", "", "", "", ""],
        ["", "", "лекції", "практичні, семінарські", "лабораторні", "індивідуальні", "самостійна"],
        ["РОЗДІЛ 1. TEST", "", "", "", "", "", ""],
        ["Тема 1.1 Intro", 10, 2, 3, 1, 0, 4],
        ["Разом за розділом 1", 10, 2, 3, 1, 0, 4],
        ["ВСЬОГО ПО НАВЧАЛЬНІЙ ДИСЦИПЛІНІ:", 10, 2, 3, 1, 0, 4],
    ]

    section_rows = etl._write_data_to_worksheet(ws, structure_data)
    etl._merge_header_cells(ws)
    etl._merge_section_cells(ws, section_rows)
    etl._apply_header_formatting(ws)
    etl._apply_content_formatting(ws)
    etl._apply_summary_row_styling(ws)
    etl._auto_adjust_column_widths(ws)

    assert 5 in section_rows
    assert "A1:A4" in [str(rng) for rng in ws.merged_cells.ranges]
    assert ws.column_dimensions["A"].width > 0


def test_generate_structure_raises_on_invalid_validation(monkeypatch):
    df = pd.DataFrame([["hdr"] * 7])
    invalid_result = ValidationResult(
        is_valid=False,
        errors=[ValidationIssue(5, "A", SeverityLevel.ERROR, "empty_field", "bad")],
        warnings=[],
        error_count=1,
        warning_count=0,
    )

    monkeypatch.setattr(etl.pd, "read_excel", lambda *args, **kwargs: df)
    monkeypatch.setattr(etl, "validate_plan_data", lambda _df: invalid_result)
    monkeypatch.setattr(etl, "format_validation_report", lambda _r: "report")
    monkeypatch.setattr(etl, "SessionLocal", lambda: FakeDbSession())
    monkeypatch.setattr(etl, "log_validation_error", lambda **kwargs: None)

    with pytest.raises(ValueError):
        etl.generate_structure("input.xlsx", output_file="out.xlsx", save_to_database=False)


def test_run_etl_pipeline_success_with_mocks(monkeypatch, tmp_path):
    df = pd.DataFrame([["hdr"] * 7])
    valid_result = ValidationResult(True, [], [], 0, 0)

    monkeypatch.setattr(etl.pd, "read_excel", lambda *args, **kwargs: df)
    monkeypatch.setattr(etl, "validate_plan_data", lambda _df: valid_result)
    monkeypatch.setattr(etl, "format_validation_report", lambda _r: "report")
    monkeypatch.setattr(etl, "SessionLocal", lambda: FakeDbSession())
    monkeypatch.setattr(etl, "load_activity_types", lambda _db: None)
    monkeypatch.setattr(etl, "load_control_forms", lambda _db: None)
    monkeypatch.setattr(etl, "find_or_create_semester", lambda _db, _num: 1)
    monkeypatch.setattr(etl, "save_section", lambda *_args, **_kwargs: 10)
    monkeypatch.setattr(etl, "save_theme", lambda *_args, **_kwargs: 20)
    monkeypatch.setattr(etl, "save_activity", lambda *_args, **_kwargs: 30)
    monkeypatch.setattr(etl, "commit_changes", lambda _db: None)
    monkeypatch.setattr(etl, "refresh_summaries", lambda _db: {"success": True, "views_refreshed": 5})
    monkeypatch.setattr(etl, "log_validation_error", lambda **kwargs: None)
    monkeypatch.setattr(
        etl,
        "_extract_and_aggregate_data",
        lambda _input: (
            ["РОЗДІЛ 1. TEST"],
            {
                ("РОЗДІЛ 1. TEST", "Тема 1.1 Intro"): {
                    "section": "РОЗДІЛ 1. TEST",
                    "theme": "Тема 1.1 Intro",
                    "total": 10,
                    "lectures": 2,
                    "practical": 3,
                    "lab": 1,
                    "individual": 0,
                    "self": 4,
                    "activities": [
                        {"name": "Лекція 1", "type_id": 1, "hours": 2, "control_form_id": 1}
                    ],
                }
            },
            {"total": 10, "lectures": 2, "practical": 3, "lab": 1, "individual": 0, "self": 4},
            5,
        ),
    )

    out_file = tmp_path / "structure.xlsx"
    stats = etl.run_etl_pipeline("input.xlsx", discipline_id=1, output_file=str(out_file), idempotent=True)

    assert stats["records_processed"] == 3
    assert stats["records_created"] == 3
    assert stats["records_updated"] == 0
    assert out_file.exists()


def test_run_etl_pipeline_raises_on_validation_errors(monkeypatch):
    df = pd.DataFrame([["hdr"] * 7])
    invalid_result = ValidationResult(
        is_valid=False,
        errors=[ValidationIssue(5, "A", SeverityLevel.ERROR, "bad", "bad")],
        warnings=[],
        error_count=1,
        warning_count=0,
    )

    monkeypatch.setattr(etl.pd, "read_excel", lambda *args, **kwargs: df)
    monkeypatch.setattr(etl, "validate_plan_data", lambda _df: invalid_result)
    monkeypatch.setattr(etl, "format_validation_report", lambda _r: "report")
    monkeypatch.setattr(etl, "SessionLocal", lambda: FakeDbSession())
    monkeypatch.setattr(etl, "log_validation_error", lambda **kwargs: None)

    with pytest.raises(ValueError):
        etl.run_etl_pipeline("input.xlsx", discipline_id=1)
