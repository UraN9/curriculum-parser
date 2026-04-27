"""
Tests for Core API Endpoints.

Tests for:
- POST /api/upload - File upload with ETL
- GET /api/topics - Topics listing
- GET /api/schedule - Schedule/Activities listing
"""

import pytest
import io
import uuid
from datetime import datetime

from app.models import (
    Activity,
    ActivityType,
    ControlForm,
    Discipline,
    Lecturer,
    Schedule,
    Section,
    Semester,
    Theme,
)


def _seed_schedule_data(db_session):
    lecturer = Lecturer(
        full_name=f"Schedule Lecturer {uuid.uuid4().hex[:6]}",
        email=f"schedule_{uuid.uuid4().hex[:8]}@test.com",
        password_hash="hash",
    )
    db_session.add(lecturer)
    db_session.commit()

    discipline = Discipline(
        name=f"Schedule Discipline {uuid.uuid4().hex[:6]}",
        course=2,
        ects_credits=5.0,
        lecturer_id=lecturer.id,
    )
    db_session.add(discipline)
    db_session.commit()

    semester = Semester(number=3, weeks=16, hours_per_week=6)
    db_session.add(semester)
    db_session.commit()

    section = Section(
        name="Section A",
        discipline_id=discipline.id,
        semester_id=semester.id,
    )
    db_session.add(section)
    db_session.commit()

    theme = Theme(name="Theme A", section_id=section.id, total_hours=8)
    db_session.add(theme)
    db_session.commit()

    activity_type = db_session.query(ActivityType).first()
    if activity_type is None:
        activity_type = ActivityType(name=f"Lecture-{uuid.uuid4().hex[:4]}")
        db_session.add(activity_type)
        db_session.commit()

    control_form = db_session.query(ControlForm).first()
    if control_form is None:
        control_form = ControlForm(name=f"Exam-{uuid.uuid4().hex[:4]}")
        db_session.add(control_form)
        db_session.commit()

    activity = Activity(
        name="Intro Lecture",
        type_id=activity_type.id,
        hours=2,
        theme_id=theme.id,
        control_form_id=control_form.id,
    )
    db_session.add(activity)
    db_session.commit()

    schedule = Schedule(
        day="monday",
        pair_number=1,
        room="101",
        activity_id=activity.id,
    )
    db_session.add(schedule)
    db_session.commit()

    return {
        "discipline": discipline,
        "activity": activity,
    }


class TestFileUploadEndpoint:
    """Tests for file upload endpoint."""
    
    def test_upload_requires_auth(self, client):
        """Test that upload requires authentication."""
        response = client.post('/api/upload')
        assert response.status_code == 401
        assert 'Authentication required' in response.json.get('error', '')
    
    def test_upload_requires_file(self, client, auth_headers):
        """Test that upload requires a file."""
        response = client.post('/api/upload', headers=auth_headers)
        assert response.status_code == 400
        assert 'No file provided' in response.json.get('message', '')
    
    def test_upload_rejects_invalid_extension(self, client, auth_headers):
        """Test that upload rejects non-Excel files."""
        data = {
            'file': (io.BytesIO(b'test content'), 'test.txt')
        }
        response = client.post(
            '/api/upload',
            headers=auth_headers,
            data=data,
            content_type='multipart/form-data'
        )
        assert response.status_code == 400
        assert 'Invalid file type' in response.json.get('message', '')
    
    def test_upload_rejects_empty_filename(self, client, auth_headers):
        """Test that upload rejects empty filename."""
        data = {
            'file': (io.BytesIO(b'test content'), '')
        }
        response = client.post(
            '/api/upload',
            headers=auth_headers,
            data=data,
            content_type='multipart/form-data'
        )
        assert response.status_code == 400


class TestTopicsEndpoint:
    """Tests for topics endpoint."""
    
    def test_topics_requires_auth(self, client):
        """Test that topics list requires authentication."""
        response = client.get('/api/topics')
        assert response.status_code == 401
        assert 'Authentication required' in response.json.get('error', '')
    
    def test_topics_list_with_auth(self, client, auth_headers):
        """Test getting topics list with authentication."""
        response = client.get('/api/topics', headers=auth_headers)
        assert response.status_code == 200
        assert 'topics' in response.json
        assert 'total' in response.json
        assert 'page' in response.json
        assert 'per_page' in response.json
    
    def test_topics_pagination(self, client, auth_headers):
        """Test topics pagination parameters."""
        response = client.get('/api/topics?page=1&per_page=5', headers=auth_headers)
        assert response.status_code == 200
        assert response.json['per_page'] == 5
        assert response.json['page'] == 1
    
    def test_topics_detail_not_found(self, client, auth_headers):
        """Test topics detail for non-existent topic."""
        response = client.get('/api/topics/99999', headers=auth_headers)
        assert response.status_code == 404
        assert 'Not Found' in response.json.get('error', '')


class TestScheduleEndpoint:
    """Tests for schedule endpoint."""
    
    def test_schedule_requires_auth(self, client):
        """Test that schedule list requires authentication."""
        response = client.get('/api/schedule')
        assert response.status_code == 401
        assert 'Authentication required' in response.json.get('error', '')
    
    def test_schedule_list_with_auth(self, client, auth_headers):
        """Test getting schedule list with authentication."""
        response = client.get('/api/schedule', headers=auth_headers)
        assert response.status_code == 200
        assert 'schedule' in response.json
        assert 'total' in response.json
        assert 'page' in response.json
    
    def test_schedule_pagination(self, client, auth_headers):
        """Test schedule pagination parameters."""
        response = client.get('/api/schedule?page=1&per_page=10', headers=auth_headers)
        assert response.status_code == 200
        assert response.json['per_page'] == 10
        assert response.json['page'] == 1
    
    def test_schedule_detail_not_found(self, client, auth_headers):
        """Test schedule detail for non-existent activity."""
        response = client.get('/api/schedule/99999', headers=auth_headers)
        assert response.status_code == 404
        assert 'Not Found' in response.json.get('error', '')
    
    def test_activity_types_list(self, client, auth_headers):
        """Test getting activity types list."""
        response = client.get('/api/schedule/types', headers=auth_headers)
        assert response.status_code == 200
        assert 'types' in response.json

    def test_schedule_build_requires_dates(self, client, auth_headers):
        response = client.get('/api/schedule/build', headers=auth_headers)
        assert response.status_code == 400
        assert response.json['error'] == 'Validation Error'

    def test_schedule_build_for_semester_interval(self, client, auth_headers, db_session):
        seeded = _seed_schedule_data(db_session)

        response = client.get(
            f"/api/schedule/build?start_date=2026-09-07&end_date=2026-09-14&discipline_id={seeded['discipline'].id}",
            headers=auth_headers,
        )

        assert response.status_code == 200
        payload = response.json
        assert payload['interval']['start_date'] == '2026-09-07'
        assert payload['interval']['end_date'] == '2026-09-14'
        assert payload['interval']['discipline_id'] == seeded['discipline'].id
        assert payload['total'] == 2
        assert len(payload['generated_schedule']) == 2
        assert payload['generated_schedule'][0]['date'] == '2026-09-07'
        assert payload['generated_schedule'][1]['date'] == '2026-09-14'
        assert payload['generated_schedule'][0]['discipline_id'] == seeded['discipline'].id

    def test_schedule_build_rejects_invalid_date_format(self, client, auth_headers):
        response = client.get(
            '/api/schedule/build?start_date=07-09-2026&end_date=2026-09-14',
            headers=auth_headers,
        )
        assert response.status_code == 400
        assert response.json['error'] == 'Validation Error'

    def test_schedule_for_specific_date_and_discipline(self, client, auth_headers, db_session):
        seeded = _seed_schedule_data(db_session)

        response = client.get(
            f"/api/schedule/discipline/{seeded['discipline'].id}/date/2026-09-07",
            headers=auth_headers,
        )

        assert response.status_code == 200
        payload = response.json
        assert payload['discipline_id'] == seeded['discipline'].id
        assert payload['date'] == '2026-09-07'
        assert payload['weekday'] == 'monday'
        assert payload['total'] == 1
        assert payload['schedule'][0]['activity_id'] == seeded['activity'].id

    def test_schedule_for_specific_date_requires_valid_date(self, client, auth_headers):
        response = client.get('/api/schedule/discipline/1/date/09-07-2026', headers=auth_headers)
        assert response.status_code == 400
        assert response.json['error'] == 'Validation Error'


class TestEndpointIntegration:
    """Integration tests for endpoints."""
    
    def test_topics_with_existing_data(self, client, auth_headers, db_session):
        """Test that topics endpoint returns existing data."""
        from app.models import Theme
        
        # Check if we have any themes
        theme_count = db_session.query(Theme).count()
        
        response = client.get('/api/topics', headers=auth_headers)
        assert response.status_code == 200
        
        # Total should match database count
        assert response.json['total'] == theme_count
    
    def test_schedule_with_existing_data(self, client, auth_headers, db_session):
        """Test that schedule endpoint returns existing data."""
        from app.models import Activity
        
        # Check if we have any activities
        activity_count = db_session.query(Activity).count()
        
        response = client.get('/api/schedule', headers=auth_headers)
        assert response.status_code == 200
        
        # Total should match database count
        assert response.json['total'] == activity_count
