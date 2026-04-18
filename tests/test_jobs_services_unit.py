"""Unit tests for ETL jobs/services modules."""

from datetime import datetime
import uuid

import pandas as pd
import pytest

from app.models import ETLError, ETLJob, ETLJobStatus
from etl_service import etl_logger
from etl_service import db_loader
from celery_app import tasks


class FakeFailingSession:
    """Minimal fake session to force logger fallback path."""

    def add(self, _obj):
        raise RuntimeError("add failed")

    def commit(self):
        raise RuntimeError("commit failed")


class FakeCommitFailSession:
    """Fake session for commit failure tests."""

    def __init__(self):
        self.rolled_back = False

    def commit(self):
        raise RuntimeError("boom")

    def rollback(self):
        self.rolled_back = True


class FakeRefreshSession:
    """Fake session for refresh_summaries tests."""

    def __init__(self, first_call_ok=False):
        self.first_call_ok = first_call_ok
        self.call_count = 0
        self.committed = False
        self.rollback_count = 0

    def execute(self, _query):
        self.call_count += 1
        if self.call_count == 1 and self.first_call_ok:
            return None
        raise RuntimeError("missing")

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rollback_count += 1


class FakeRequest:
    def __init__(self, task_id):
        self.id = task_id


class FakeTaskSelf:
    """Fake Celery task self for helper-level tests."""

    def __init__(self, task_id="unit-task-id"):
        self.request = FakeRequest(task_id)
        self.states = []

    def update_state(self, **kwargs):
        self.states.append(kwargs)


def test_etl_session_string_contains_file_name():
    session = etl_logger.ETLSession(file_name="plan.xlsx")
    text = str(session)
    assert "ETLSession" in text
    assert "plan.xlsx" in text


def test_log_validation_error_persists_record(db_session):
    session_id = uuid.uuid4()

    etl_logger.log_validation_error(
        db_session=db_session,
        message="invalid hours",
        row_number=12,
        field_name="total_hours",
        source_data={"total": -2},
        etl_session_id=session_id,
        file_name="input.xlsx",
        severity=etl_logger.SEVERITY_WARNING,
    )

    row = db_session.query(ETLError).order_by(ETLError.id.desc()).first()
    assert row is not None
    assert row.message == "invalid hours"
    assert row.row_number == 12
    assert row.file_name == "input.xlsx"


def test_get_session_errors_and_filter(db_session):
    session_id = uuid.uuid4()

    etl_logger.log_validation_error(
        db_session,
        message="warning 1",
        etl_session_id=session_id,
        severity=etl_logger.SEVERITY_WARNING,
    )
    etl_logger.log_validation_error(
        db_session,
        message="error 1",
        etl_session_id=session_id,
        severity=etl_logger.SEVERITY_ERROR,
    )

    all_errors = etl_logger.get_session_errors(db_session, session_id)
    only_errors = etl_logger.get_session_errors(
        db_session, session_id, severity_filter=etl_logger.SEVERITY_ERROR
    )

    assert len(all_errors) >= 2
    assert len(only_errors) >= 1
    assert all(item["severity"] == "error" for item in only_errors)


def test_get_recent_errors_with_limit(db_session):
    etl_logger.log_validation_error(db_session, message="recent-a")
    etl_logger.log_validation_error(db_session, message="recent-b")

    recent = etl_logger.get_recent_errors(db_session, limit=1)

    assert len(recent) == 1
    assert "message" in recent[0]
    assert "severity" in recent[0]


def test_format_error_report_handles_empty_and_non_empty():
    empty_report = etl_logger.format_error_report([], session_id="abc")
    assert "No errors found" in empty_report

    report = etl_logger.format_error_report(
        [
            {
                "id": 1,
                "timestamp": datetime.utcnow(),
                "error_type": "validation",
                "severity": "error",
                "row_number": 5,
                "field_name": "hours",
                "message": "bad value",
                "source_data": "-1",
                "file_name": "f.xlsx",
            }
        ],
        session_id="abc",
    )
    assert "ETL ERROR REPORT" in report
    assert "Errors:" in report
    assert "bad value" in report


def test_logger_fallback_prints_when_db_write_fails(capsys):
    etl_logger.log_validation_error(
        db_session=FakeFailingSession(),
        message="will fail",
    )
    captured = capsys.readouterr()
    assert "Failed to log error to database" in captured.out


def test_extract_semester_number_cases():
    assert db_loader.extract_semester_number("5 SEMESTER") == 5
    assert db_loader.extract_semester_number("SEMESTER 3") == 3
    assert db_loader.extract_semester_number("invalid") is None
    assert db_loader.extract_semester_number("") is None


def test_extract_activity_type_cases():
    assert db_loader.extract_activity_type("Lecture 1") is None
    assert db_loader.extract_activity_type("Lektion 1") is None
    assert db_loader.extract_activity_type(None) is None
    assert db_loader.extract_activity_type("Лекція 1") == 1
    assert db_loader.extract_activity_type("Практична робота") == 2
    assert db_loader.extract_activity_type("Семінарська робота") == 2
    assert db_loader.extract_activity_type("Лабораторна робота") == 3
    assert db_loader.extract_activity_type("Самостійна робота") == 4


def test_extract_control_form_cases():
    assert db_loader.extract_control_form(None) is None
    assert db_loader.extract_control_form("") is None
    assert db_loader.extract_control_form(pd.NA) is None
    assert db_loader.extract_control_form("опитування") == 1
    assert db_loader.extract_control_form("Захист проекту") == 2
    assert db_loader.extract_control_form("КОНСПЕКТ") == 3
    assert db_loader.extract_control_form("unknown") is None


def test_commit_changes_rolls_back_and_raises():
    fake = FakeCommitFailSession()
    with pytest.raises(RuntimeError):
        db_loader.commit_changes(fake)
    assert fake.rolled_back is True


def test_refresh_summaries_reports_missing_views():
    fake = FakeRefreshSession(first_call_ok=False)
    result = db_loader.refresh_summaries(fake)
    assert result["success"] is False
    assert result["views_refreshed"] == 0
    assert result["error"] is not None


def test_refresh_summaries_success_via_function_call():
    fake = FakeRefreshSession(first_call_ok=True)
    result = db_loader.refresh_summaries(fake)
    assert result["success"] is True
    assert result["views_refreshed"] == 5


def test_get_or_create_job_creates_and_reuses(db_session):
    task_id = f"task-{uuid.uuid4().hex[:8]}"

    created = tasks._get_or_create_job(
        db=db_session,
        task_id=task_id,
        input_file="C:/tmp/plan.xlsx",
        discipline_id=None,
        user_id=7,
    )
    reused = tasks._get_or_create_job(
        db=db_session,
        task_id=task_id,
        input_file="C:/tmp/plan.xlsx",
        discipline_id=None,
        user_id=7,
    )

    assert created.id == reused.id
    assert created.input_file == "plan.xlsx"
    assert created.status == ETLJobStatus.PENDING


def test_handle_task_failure_updates_job(db_session):
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    job = ETLJob(task_id=task_id, input_file="file.xlsx", status=ETLJobStatus.RUNNING)
    db_session.add(job)
    db_session.commit()

    tasks._handle_task_failure(db_session, task_id, "failed now")

    db_session.refresh(job)
    assert job.status == ETLJobStatus.FAILED
    assert "failed now" in job.error_message
    assert job.completed_at is not None


def test_check_etl_health_returns_expected_keys():
    data = tasks.check_etl_health.run()
    assert data["status"] == "healthy"
    assert data["service"] == "celery-etl"
    assert "timestamp" in data
