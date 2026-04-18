"""Additional model helper tests for coverage."""

from datetime import datetime

from app.models import ETLError, ETLJob, ETLJobStatus, ErrorTypeEnum, SeverityEnum


def test_etl_error_repr_truncates_message():
    msg = "x" * 80
    err = ETLError(
        error_type=ErrorTypeEnum.validation,
        severity=SeverityEnum.error,
        message=msg,
    )

    text = repr(err)
    assert "ETLError" in text
    assert "error_type" in text
    assert "..." in text


def test_etl_job_repr_and_duration_none():
    job = ETLJob(task_id="task-abc", input_file="f.xlsx", status=ETLJobStatus.PENDING)

    assert "ETLJob" in repr(job)
    assert job.duration_seconds is None


def test_etl_job_duration_seconds_value():
    job = ETLJob(task_id="task-def", input_file="f.xlsx", status=ETLJobStatus.COMPLETED)
    job.started_at = datetime(2026, 1, 1, 10, 0, 0)
    job.completed_at = datetime(2026, 1, 1, 10, 2, 30)

    assert job.duration_seconds == 150.0
