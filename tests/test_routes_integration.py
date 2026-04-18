"""Integration tests for controllers/routes endpoints."""

import io
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.models import Discipline, ETLJob, ETLJobStatus, Lecturer


class _DummyAsyncResult:
    def __init__(self, status, info=None, result=None):
        self.status = status
        self.info = info
        self.result = result


def _create_discipline(db_session):
    lecturer = Lecturer(
        full_name=f"Route Lecturer {uuid.uuid4().hex[:6]}",
        email=f"route_lect_{uuid.uuid4().hex[:8]}@test.com",
        password_hash="hash123",
    )
    db_session.add(lecturer)
    db_session.commit()

    discipline = Discipline(
        name=f"Route Discipline {uuid.uuid4().hex[:6]}",
        course=2,
        ects_credits=5.0,
        lecturer_id=lecturer.id,
    )
    db_session.add(discipline)
    db_session.commit()
    return discipline


class TestSectionsRoutesIntegration:
    def test_sections_list_not_implemented(self, client):
        response = client.get("/api/sections")
        assert response.status_code == 501

    def test_sections_create_not_implemented(self, client):
        response = client.post("/api/sections", json={"name": "S"})
        assert response.status_code == 501

    def test_sections_detail_not_implemented(self, client):
        response = client.get("/api/sections/1")
        assert response.status_code == 501

    def test_sections_update_not_implemented(self, client):
        response = client.put("/api/sections/1", json={"name": "X"})
        assert response.status_code == 501

    def test_sections_delete_not_implemented(self, client):
        response = client.delete("/api/sections/1")
        assert response.status_code == 501


class TestDisciplinesRoutesIntegration:
    def test_disciplines_get_list_returns_array(self, client):
        response = client.get("/api/disciplines")
        assert response.status_code == 200
        assert isinstance(response.get_json(), list)

    def test_disciplines_post_no_data_returns_400(self, client):
        response = client.post("/api/disciplines", json={})
        assert response.status_code == 400

    def test_disciplines_get_by_id_not_found(self, client):
        response = client.get("/api/disciplines/999999")
        assert response.status_code == 404

    def test_disciplines_put_no_data_returns_400(self, client, db_session):
        discipline = _create_discipline(db_session)
        response = client.put(f"/api/disciplines/{discipline.id}", json={})
        assert response.status_code == 400

    def test_disciplines_delete_not_found(self, client):
        response = client.delete("/api/disciplines/999999")
        assert response.status_code == 404


class TestUploadRouteIntegration:
    def test_upload_discipline_not_found(self, client, auth_headers):
        data = {
            "file": (io.BytesIO(b"excel-content"), "plan.xlsx"),
            "discipline_id": 999999,
        }
        response = client.post(
            "/api/upload",
            headers=auth_headers,
            data=data,
            content_type="multipart/form-data",
        )
        assert response.status_code == 404

    def test_upload_async_start_returns_202(self, client, auth_headers, db_session, monkeypatch):
        discipline = _create_discipline(db_session)

        class _Task:
            @staticmethod
            def delay(*_args, **_kwargs):
                return None

        monkeypatch.setattr("celery_app.tasks.run_etl_task", _Task)

        data = {
            "file": (io.BytesIO(b"excel-content"), "plan.xlsx"),
            "discipline_id": str(discipline.id),
        }
        response = client.post(
            "/api/upload",
            headers=auth_headers,
            data=data,
            content_type="multipart/form-data",
        )

        assert response.status_code == 202
        payload = response.get_json()
        assert payload["status"] == "pending"
        assert "task_id" in payload
        assert "job_id" in payload

    def test_upload_sync_fallback_success_returns_201(self, client, auth_headers, db_session, monkeypatch):
        discipline = _create_discipline(db_session)

        class _BrokenTask:
            @staticmethod
            def delay(*_args, **_kwargs):
                raise RuntimeError("celery unavailable")

        monkeypatch.setattr("celery_app.tasks.run_etl_task", _BrokenTask)
        monkeypatch.setattr(
            "etl_service.etl.run_etl_pipeline",
            lambda *_args, **_kwargs: {
                "records_processed": 3,
                "records_created": 1,
                "records_updated": 2,
                "records_skipped": 0,
            },
        )

        data = {
            "file": (io.BytesIO(b"excel-content"), "plan.xlsx"),
            "discipline_id": str(discipline.id),
        }
        response = client.post(
            "/api/upload",
            headers=auth_headers,
            data=data,
            content_type="multipart/form-data",
        )

        assert response.status_code == 201
        payload = response.get_json()
        assert payload["status"] == "completed"
        assert "result" in payload

    def test_upload_sync_fallback_failure_returns_500(self, client, auth_headers, db_session, monkeypatch):
        discipline = _create_discipline(db_session)

        class _BrokenTask:
            @staticmethod
            def delay(*_args, **_kwargs):
                raise RuntimeError("celery unavailable")

        monkeypatch.setattr("celery_app.tasks.run_etl_task", _BrokenTask)

        def _raise(*_args, **_kwargs):
            raise RuntimeError("etl crashed")

        monkeypatch.setattr("etl_service.etl.run_etl_pipeline", _raise)

        data = {
            "file": (io.BytesIO(b"excel-content"), "plan.xlsx"),
            "discipline_id": str(discipline.id),
        }
        response = client.post(
            "/api/upload",
            headers=auth_headers,
            data=data,
            content_type="multipart/form-data",
        )

        assert response.status_code == 500
        payload = response.get_json()
        assert payload["status"] == "failed"


class TestETLRoutesIntegration:
    def test_etl_start_no_data_returns_400(self, client, auth_headers):
        response = client.post(
            "/api/etl/start",
            headers=auth_headers,
            data="null",
            content_type="application/json",
        )
        assert response.status_code == 400

    def test_etl_start_discipline_not_found(self, client, auth_headers):
        response = client.post(
            "/api/etl/start",
            headers=auth_headers,
            json={"input_file": "f.xlsx", "discipline_id": 999999},
        )
        assert response.status_code == 404

    def test_etl_start_success_returns_202(self, client, auth_headers, db_session, monkeypatch):
        discipline = _create_discipline(db_session)

        class _Task:
            @staticmethod
            def delay(**_kwargs):
                return SimpleNamespace(id="task-123")

        monkeypatch.setattr("celery_app.tasks.run_etl_task", _Task)

        response = client.post(
            "/api/etl/start",
            headers=auth_headers,
            json={"input_file": "f.xlsx", "discipline_id": discipline.id},
        )

        assert response.status_code == 202
        payload = response.get_json()
        assert payload["task_id"] == "task-123"

    def test_etl_start_failure_returns_500(self, client, auth_headers, db_session, monkeypatch):
        discipline = _create_discipline(db_session)

        class _Task:
            @staticmethod
            def delay(**_kwargs):
                raise RuntimeError("queue down")

        monkeypatch.setattr("celery_app.tasks.run_etl_task", _Task)

        response = client.post(
            "/api/etl/start",
            headers=auth_headers,
            json={"input_file": "f.xlsx", "discipline_id": discipline.id},
        )

        assert response.status_code == 500

    @pytest.mark.parametrize(
        "status,info,result,expected_progress",
        [
            ("PENDING", None, None, 0),
            ("STARTED", None, None, 10),
            ("PROGRESS", {"status": "Extracting", "progress": 35}, None, 35),
            ("SUCCESS", None, {"ok": True}, 100),
            ("FAILURE", None, RuntimeError("failed"), 0),
        ],
    )
    def test_etl_status_celery_branches(
        self, client, auth_headers, monkeypatch, status, info, result, expected_progress
    ):
        monkeypatch.setattr(
            "celery.result.AsyncResult",
            lambda *_args, **_kwargs: _DummyAsyncResult(status=status, info=info, result=result),
        )

        response = client.get("/api/etl/status/task-id", headers=auth_headers)
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["status"] == status
        assert payload["progress"] == expected_progress

    def test_etl_status_fallback_db_not_found(self, client, auth_headers, monkeypatch):
        def _raise(*_args, **_kwargs):
            raise RuntimeError("celery down")

        monkeypatch.setattr("celery.result.AsyncResult", _raise)

        response = client.get("/api/etl/status/unknown-task", headers=auth_headers)
        assert response.status_code == 404

    def test_etl_status_fallback_db_completed(self, client, auth_headers, db_session, monkeypatch):
        def _raise(*_args, **_kwargs):
            raise RuntimeError("celery down")

        monkeypatch.setattr("celery.result.AsyncResult", _raise)

        task_id = f"task-{uuid.uuid4().hex[:8]}"
        job = ETLJob(
            task_id=task_id,
            input_file="f.xlsx",
            status=ETLJobStatus.COMPLETED,
            records_processed=10,
            records_created=4,
            records_updated=6,
            records_skipped=0,
        )
        db_session.add(job)
        db_session.commit()

        response = client.get(f"/api/etl/status/{task_id}", headers=auth_headers)
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["status"] == "completed"
        assert payload["progress"] == 100
        assert "statistics" in payload

    def test_etl_jobs_invalid_status_filter(self, client, auth_headers):
        response = client.get("/api/etl/jobs?status=not_real", headers=auth_headers)
        assert response.status_code == 400

    def test_etl_job_detail_not_found(self, client, auth_headers):
        response = client.get("/api/etl/jobs/999999", headers=auth_headers)
        assert response.status_code == 404

    def test_etl_health_degraded_when_queue_down(self, client, monkeypatch):
        class _Task:
            @staticmethod
            def delay():
                raise RuntimeError("broker down")

        monkeypatch.setattr("celery_app.tasks.check_etl_health", _Task)

        response = client.get("/api/etl/health")
        assert response.status_code == 503
        assert response.get_json()["status"] == "degraded"

    def test_etl_health_healthy(self, client, monkeypatch):
        class _Result:
            @staticmethod
            def get(timeout=5):
                return {"status": "healthy"}

        class _Task:
            @staticmethod
            def delay():
                return _Result()

        monkeypatch.setattr("celery_app.tasks.check_etl_health", _Task)

        response = client.get("/api/etl/health")
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["status"] == "healthy"
