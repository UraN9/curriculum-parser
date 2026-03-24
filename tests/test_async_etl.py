"""
Tests for Asynchronous ETL functionality.

Tests cover:
- ETL idempotency
- Celery task creation and tracking
- API endpoints for ETL management
- ETLJob model
"""

import pytest
import uuid
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from app.models import ETLJob, ETLJobStatus, Discipline, Lecturer, Section, Theme, Activity
from app.database import SessionLocal


def unique_task_id():
    """Generate unique task ID for tests."""
    return f"test-task-{uuid.uuid4().hex[:12]}"


def unique_email(prefix="test"):
    """Generate unique email for tests."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}@test.com"


class TestETLJobModel:
    """Tests for ETLJob model."""
    
    def test_create_etl_job(self, db_session):
        """Test creating an ETL job record."""
        task_id = unique_task_id()
        job = ETLJob(
            task_id=task_id,
            input_file="test.xlsx",
            discipline_id=None,
            status=ETLJobStatus.PENDING
        )
        db_session.add(job)
        db_session.commit()
        
        assert job.id is not None
        assert job.task_id == task_id
        assert job.status == ETLJobStatus.PENDING
        assert job.created_at is not None
    
    def test_etl_job_status_transitions(self, db_session):
        """Test ETL job status can be updated."""
        job = ETLJob(
            task_id=unique_task_id(),
            input_file="test.xlsx",
            status=ETLJobStatus.PENDING
        )
        db_session.add(job)
        db_session.commit()
        
        # Transition to RUNNING
        job.status = ETLJobStatus.RUNNING
        job.started_at = datetime.now(timezone.utc)
        db_session.commit()
        
        assert job.status == ETLJobStatus.RUNNING
        assert job.started_at is not None
        
        # Transition to COMPLETED
        job.status = ETLJobStatus.COMPLETED
        job.completed_at = datetime.now(timezone.utc)
        job.records_processed = 100
        job.records_created = 50
        job.records_updated = 50
        db_session.commit()
        
        assert job.status == ETLJobStatus.COMPLETED
        assert job.records_processed == 100
    
    def test_etl_job_duration(self, db_session):
        """Test ETL job duration calculation."""
        job = ETLJob(
            task_id=unique_task_id(),
            input_file="test.xlsx",
            status=ETLJobStatus.RUNNING
        )
        job.started_at = datetime(2026, 3, 16, 10, 0, 0)
        job.completed_at = datetime(2026, 3, 16, 10, 5, 30)
        
        assert job.duration_seconds == 330.0  # 5 minutes 30 seconds
    
    def test_etl_job_unique_task_id(self, db_session):
        """Test that task_id must be unique."""
        shared_task_id = unique_task_id()
        
        job1 = ETLJob(
            task_id=shared_task_id,
            input_file="test1.xlsx",
            status=ETLJobStatus.PENDING
        )
        db_session.add(job1)
        db_session.commit()
        
        job2 = ETLJob(
            task_id=shared_task_id,  # Same task_id
            input_file="test2.xlsx",
            status=ETLJobStatus.PENDING
        )
        db_session.add(job2)
        
        with pytest.raises(Exception):  # IntegrityError
            db_session.commit()


class TestETLIdempotency:
    """Tests for ETL idempotency."""
    
    def test_save_section_idempotent(self, db_session):
        """Test that saving the same section twice doesn't create duplicates."""
        from etl_service.db_loader import save_section, find_or_create_semester
        
        # Create prerequisite data
        lecturer = Lecturer(
            full_name="Test Lecturer",
            email=unique_email("lecturer_idem"),
            password_hash="hash123"
        )
        db_session.add(lecturer)
        db_session.commit()
        
        discipline = Discipline(
            name="Test Discipline",
            course=1,
            ects_credits=5.0,
            lecturer_id=lecturer.id
        )
        db_session.add(discipline)
        db_session.commit()
        
        semester_id = find_or_create_semester(db_session, 5)
        
        # Save section first time
        section_name = f"РОЗДІЛ 1. Тестовий розділ {uuid.uuid4().hex[:6]}"
        section_id_1 = save_section(
            db_session,
            name=section_name,
            discipline_id=discipline.id,
            semester_id=semester_id
        )
        db_session.commit()
        
        # Save same section second time
        section_id_2 = save_section(
            db_session,
            name=section_name,
            discipline_id=discipline.id,
            semester_id=semester_id
        )
        db_session.commit()
        
        # Should return same ID
        assert section_id_1 == section_id_2
        
        # Should only have one section with this name
        count = db_session.query(Section).filter_by(name=section_name).count()
        assert count == 1
    
    def test_save_theme_idempotent_updates_hours(self, db_session):
        """Test that saving the same theme updates hours instead of duplicating."""
        from etl_service.db_loader import save_section, save_theme, find_or_create_semester
        
        # Create prerequisite data
        lecturer = Lecturer(
            full_name="Test Lecturer 2",
            email=unique_email("lecturer_idem2"),
            password_hash="hash123"
        )
        db_session.add(lecturer)
        db_session.commit()
        
        discipline = Discipline(
            name="Test Discipline 2",
            course=2,
            ects_credits=4.0,
            lecturer_id=lecturer.id
        )
        db_session.add(discipline)
        db_session.commit()
        
        semester_id = find_or_create_semester(db_session, 6)
        section_id = save_section(
            db_session,
            name=f"РОЗДІЛ 2. Тестовий {uuid.uuid4().hex[:6]}",
            discipline_id=discipline.id,
            semester_id=semester_id
        )
        db_session.commit()
        
        # Save theme with 10 hours
        theme_name = f"Тема 2.1 Тестова тема {uuid.uuid4().hex[:6]}"
        theme_id_1 = save_theme(
            db_session,
            name=theme_name,
            section_id=section_id,
            total_hours=10
        )
        db_session.commit()
        
        # Save same theme with 20 hours (update)
        theme_id_2 = save_theme(
            db_session,
            name=theme_name,
            section_id=section_id,
            total_hours=20
        )
        db_session.commit()
        
        # Should return same ID
        assert theme_id_1 == theme_id_2
        
        # Hours should be updated
        theme = db_session.query(Theme).filter_by(id=theme_id_1).first()
        assert theme.total_hours == 20
    
    def test_save_activity_idempotent(self, db_session):
        """Test that saving the same activity updates instead of duplicating."""
        from etl_service.db_loader import (
            save_section, save_theme, save_activity,
            find_or_create_semester, load_activity_types
        )
        
        # Load reference data
        load_activity_types(db_session)
        
        # Create prerequisite data
        lecturer = Lecturer(
            full_name="Test Lecturer 3",
            email=unique_email("lecturer_idem3"),
            password_hash="hash123"
        )
        db_session.add(lecturer)
        db_session.commit()
        
        discipline = Discipline(
            name="Test Discipline 3",
            course=3,
            ects_credits=3.0,
            lecturer_id=lecturer.id
        )
        db_session.add(discipline)
        db_session.commit()
        
        semester_id = find_or_create_semester(db_session, 7)
        section_id = save_section(
            db_session,
            name=f"РОЗДІЛ 3. Тестовий {uuid.uuid4().hex[:6]}",
            discipline_id=discipline.id,
            semester_id=semester_id
        )
        theme_id = save_theme(
            db_session,
            name=f"Тема 3.1 Тестова {uuid.uuid4().hex[:6]}",
            section_id=section_id,
            total_hours=15
        )
        db_session.commit()
        
        # Save activity with 2 hours
        activity_name = f"Лекція 1. Вступ {uuid.uuid4().hex[:6]}"
        activity_id_1 = save_activity(
            db_session,
            name=activity_name,
            type_id=1,
            hours=2,
            theme_id=theme_id
        )
        db_session.commit()
        
        # Save same activity with 4 hours (update)
        activity_id_2 = save_activity(
            db_session,
            name=activity_name,
            type_id=1,
            hours=4,
            theme_id=theme_id
        )
        db_session.commit()
        
        # Should return same ID
        assert activity_id_1 == activity_id_2
        
        # Hours should be updated
        activity = db_session.query(Activity).filter_by(id=activity_id_1).first()
        assert activity.hours == 4


class TestETLAPIEndpoints:
    """Tests for ETL API endpoints."""
    
    def test_etl_jobs_list_requires_auth(self, client):
        """Test that ETL jobs list requires authentication."""
        response = client.get('/api/etl/jobs')
        assert response.status_code == 401
        data = response.get_json()
        assert 'error' in data
    
    def test_etl_start_requires_auth(self, client):
        """Test that starting ETL requires authentication."""
        response = client.post('/api/etl/start', json={
            "input_file": "test.xlsx",
            "discipline_id": 1
        })
        assert response.status_code == 401
    
    def test_etl_status_requires_auth(self, client):
        """Test that ETL status check requires authentication."""
        response = client.get('/api/etl/status/test-task-id')
        assert response.status_code == 401
    
    def test_etl_jobs_list_with_auth(self, client, auth_headers):
        """Test ETL jobs list with authentication."""
        response = client.get('/api/etl/jobs', headers=auth_headers)
        assert response.status_code == 200
        
        data = response.get_json()
        assert 'jobs' in data
        assert 'total' in data
        assert 'page' in data
    
    def test_etl_start_missing_input_file(self, client, auth_headers):
        """Test ETL start with missing input_file."""
        response = client.post('/api/etl/start', 
            json={"discipline_id": 1},
            headers=auth_headers
        )
        assert response.status_code == 400
        assert 'input_file is required' in response.get_json().get('error', '')
    
    def test_etl_start_missing_discipline_id(self, client, auth_headers):
        """Test ETL start with missing discipline_id."""
        response = client.post('/api/etl/start',
            json={"input_file": "test.xlsx"},
            headers=auth_headers
        )
        assert response.status_code == 400
        assert 'discipline_id is required' in response.get_json().get('error', '')
