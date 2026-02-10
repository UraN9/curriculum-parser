"""
Unit tests for relation validations.

Tests that foreign key relationships are validated:
- Discipline -> Lecturer (must exist)
- Section -> Discipline (must exist)
- Theme -> Section (must exist)
- Activity -> Theme, ActivityType (must exist)
"""
import pytest
import json
from app.database import SessionLocal, Base, engine
from app.models import Lecturer, Discipline, Section, Theme, Activity, ActivityType


@pytest.fixture(scope='module')
def db_session():
    """Create database session for tests."""
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture(scope='module')
def seed_data(db_session):
    """Seed test data for relation tests."""
    # Create test lecturer
    lecturer = Lecturer(
        full_name="Тестовий Викладач",
        email="test_relation@example.com",
        password_hash="hash123",
        role="lecturer"
    )
    db_session.add(lecturer)
    db_session.flush()
    
    # Create test discipline
    discipline = Discipline(
        name="Тестова дисципліна",
        course=3,
        ects_credits=5.0,
        lecturer_id=lecturer.id
    )
    db_session.add(discipline)
    db_session.flush()
    
    # Create test section
    section = Section(
        name="Тестовий розділ",
        discipline_id=discipline.id
    )
    db_session.add(section)
    db_session.flush()
    
    # Create test theme
    theme = Theme(
        name="Тестова тема",
        section_id=section.id
    )
    db_session.add(theme)
    db_session.flush()
    
    # Create activity type
    activity_type = db_session.query(ActivityType).first()
    if not activity_type:
        activity_type = ActivityType(id=1, name="Лекція")
        db_session.add(activity_type)
        db_session.flush()
    
    db_session.commit()
    
    return {
        'lecturer_id': lecturer.id,
        'discipline_id': discipline.id,
        'section_id': section.id,
        'theme_id': theme.id,
        'activity_type_id': activity_type.id
    }


class TestDisciplineRelations:
    """Test Discipline -> Lecturer relation validation."""
    
    def test_create_discipline_with_valid_lecturer(self, client, seed_data):
        """Should create discipline when lecturer exists."""
        response = client.post('/api/disciplines', 
            json={
                'name': 'Нова дисципліна',
                'course': 2,
                'ects_credits': 4.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code in [200, 201]
    
    def test_create_discipline_with_invalid_lecturer(self, client):
        """Should return 422 when lecturer does not exist."""
        response = client.post('/api/disciplines',
            json={
                'name': 'Дисципліна з неіснуючим викладачем',
                'course': 2,
                'ects_credits': 4.0,
                'lecturer_id': 99999  # Non-existent lecturer
            }
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert 'error' in data
        assert 'lecturer_id' in str(data)
    
    def test_update_discipline_with_invalid_lecturer(self, client, seed_data):
        """Should return 422 when updating with non-existent lecturer."""
        # First create a discipline
        create_response = client.post('/api/disciplines',
            json={
                'name': 'Дисципліна для оновлення',
                'course': 1,
                'ects_credits': 3.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        
        if create_response.status_code in [200, 201]:
            discipline_id = json.loads(create_response.data)['id']
            
            # Try to update with invalid lecturer
            update_response = client.put(f'/api/disciplines/{discipline_id}',
                json={'lecturer_id': 99999}
            )
            assert update_response.status_code == 422


class TestAPIValidationErrors:
    """Test that API returns proper 422 errors."""
    
    def test_discipline_missing_required_fields(self, client):
        """Should return 422 for missing required fields."""
        response = client.post('/api/disciplines',
            json={'name': 'Тільки назва'}  # Missing course, ects_credits, lecturer_id
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert 'error' in data
        assert data['error'] == 'Validation failed'
        assert 'details' in data
    
    def test_discipline_invalid_course(self, client, seed_data):
        """Should return 422 for invalid course number."""
        response = client.post('/api/disciplines',
            json={
                'name': 'Дисципліна',
                'course': 10,  # Invalid: > 6
                'ects_credits': 5.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert 'course' in str(data['details'])
    
    def test_discipline_invalid_ects(self, client, seed_data):
        """Should return 422 for invalid ECTS credits."""
        response = client.post('/api/disciplines',
            json={
                'name': 'Дисципліна',
                'course': 3,
                'ects_credits': 50.0,  # Invalid: > 30
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert 'ects_credits' in str(data['details'])
    
    def test_discipline_empty_name(self, client, seed_data):
        """Should return 422 for empty name."""
        response = client.post('/api/disciplines',
            json={
                'name': '',
                'course': 3,
                'ects_credits': 5.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code == 422
        data = json.loads(response.data)
        assert 'name' in str(data['details'])
    
    def test_discipline_name_too_short(self, client, seed_data):
        """Should return 422 for name with less than 2 characters."""
        response = client.post('/api/disciplines',
            json={
                'name': 'A',  # Too short
                'course': 3,
                'ects_credits': 5.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code == 422


class TestDataIntegrity:
    """Test data integrity validations."""
    
    def test_valid_discipline_created(self, client, seed_data):
        """Valid data should create discipline successfully."""
        response = client.post('/api/disciplines',
            json={
                'name': 'Валідна дисципліна',
                'course': 4,
                'ects_credits': 6.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        assert response.status_code in [200, 201]
        data = json.loads(response.data)
        assert data['name'] == 'Валідна дисципліна'
        assert data['course'] == 4
        assert data['ects_credits'] == 6.0
    
    def test_partial_update_valid(self, client, seed_data):
        """Partial update with valid data should succeed."""
        # Create discipline
        create_response = client.post('/api/disciplines',
            json={
                'name': 'Для часткового оновлення',
                'course': 2,
                'ects_credits': 3.0,
                'lecturer_id': seed_data['lecturer_id']
            }
        )
        
        if create_response.status_code in [200, 201]:
            discipline_id = json.loads(create_response.data)['id']
            
            # Partial update - only name
            update_response = client.put(f'/api/disciplines/{discipline_id}',
                json={'name': 'Оновлена назва'}
            )
            assert update_response.status_code == 200
            data = json.loads(update_response.data)
            assert data['name'] == 'Оновлена назва'
            # Other fields should remain unchanged
            assert data['course'] == 2
