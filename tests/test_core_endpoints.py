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
