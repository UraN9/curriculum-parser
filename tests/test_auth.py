"""
Unit tests for Authentication and Authorization.

Tests cover:
- User registration (student/lecturer)
- User login with JWT token
- Token validation
- Role-based access control
- Password hashing
"""

import pytest
from flask import g
from sqlalchemy.orm import Session

from app.database import engine
from app.models import Lecturer, Student, RoleEnum
from api.auth import (
    hash_password,
    verify_password,
    generate_token,
    decode_token,
    authenticate_user,
    register_user
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="function")
def db_session():
    """Create a database session for testing."""
    session = Session(bind=engine)
    # Clean up test users before each test
    session.query(Lecturer).filter(Lecturer.email.like('%@test.auth.com')).delete()
    session.query(Student).filter(Student.email.like('%@test.auth.com')).delete()
    session.commit()
    yield session
    # Clean up after test
    session.query(Lecturer).filter(Lecturer.email.like('%@test.auth.com')).delete()
    session.query(Student).filter(Student.email.like('%@test.auth.com')).delete()
    session.commit()
    session.close()


# ============================================================================
# Password Hashing Tests
# ============================================================================

class TestPasswordHashing:
    """Tests for password hashing functions."""
    
    def test_hash_password_returns_hex_string(self):
        """Hash should return a 64-character hex string (SHA256)."""
        password = "test_password_123"
        hashed = hash_password(password)
        
        assert isinstance(hashed, str)
        assert len(hashed) == 64  # SHA256 produces 64 hex characters
    
    def test_hash_password_is_deterministic(self):
        """Same password should always produce same hash."""
        password = "same_password"
        hash1 = hash_password(password)
        hash2 = hash_password(password)
        
        assert hash1 == hash2
    
    def test_different_passwords_produce_different_hashes(self):
        """Different passwords should produce different hashes."""
        hash1 = hash_password("password1")
        hash2 = hash_password("password2")
        
        assert hash1 != hash2
    
    def test_verify_password_correct(self):
        """Verify should return True for correct password."""
        password = "correct_password"
        hashed = hash_password(password)
        
        assert verify_password(password, hashed) is True
    
    def test_verify_password_incorrect(self):
        """Verify should return False for incorrect password."""
        hashed = hash_password("correct_password")
        
        assert verify_password("wrong_password", hashed) is False


# ============================================================================
# JWT Token Tests
# ============================================================================

class TestJWTTokens:
    """Tests for JWT token generation and validation."""
    
    def test_generate_token_returns_string(self):
        """Generate token should return a string."""
        token = generate_token(
            user_id=1,
            email="test@example.com",
            role="admin",
            user_type="lecturer"
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_decode_token_returns_payload(self):
        """Decode should return the original payload data."""
        token = generate_token(
            user_id=42,
            email="user@test.auth.com",
            role="viewer",
            user_type="student"
        )
        
        payload = decode_token(token)
        
        assert payload['user_id'] == 42
        assert payload['email'] == "user@test.auth.com"
        assert payload['role'] == "viewer"
        assert payload['user_type'] == "student"
    
    def test_decode_token_contains_expiration(self):
        """Token payload should contain expiration time."""
        token = generate_token(
            user_id=1,
            email="test@test.auth.com",
            role="admin",
            user_type="lecturer"
        )
        
        payload = decode_token(token)
        
        assert 'exp' in payload
        assert 'iat' in payload
    
    def test_decode_invalid_token_raises_error(self):
        """Decoding invalid token should raise an error."""
        import jwt
        
        with pytest.raises(jwt.InvalidTokenError):
            decode_token("invalid.token.here")


# ============================================================================
# User Registration Tests
# ============================================================================

class TestUserRegistration:
    """Tests for user registration."""
    
    def test_register_student_success(self, db_session):
        """Should successfully register a new student."""
        user, error = register_user(
            email="new_student@test.auth.com",
            password="password123",
            full_name="Test Student",
            user_type="student",
            role=RoleEnum.viewer
        )
        
        assert error is None
        assert user is not None
        assert user.email == "new_student@test.auth.com"
        assert user.full_name == "Test Student"
        assert user.role == RoleEnum.viewer
    
    def test_register_lecturer_success(self, db_session):
        """Should successfully register a new lecturer."""
        user, error = register_user(
            email="new_lecturer@test.auth.com",
            password="password123",
            full_name="Test Lecturer",
            user_type="lecturer",
            role=RoleEnum.lecturer
        )
        
        assert error is None
        assert user is not None
        assert user.email == "new_lecturer@test.auth.com"
        assert user.role == RoleEnum.lecturer
    
    def test_register_duplicate_email_fails(self, db_session):
        """Should fail when registering with existing email."""
        # Register first user
        register_user(
            email="duplicate@test.auth.com",
            password="password123",
            full_name="First User",
            user_type="student"
        )
        
        # Try to register with same email
        user, error = register_user(
            email="duplicate@test.auth.com",
            password="different_password",
            full_name="Second User",
            user_type="student"
        )
        
        assert user is None
        assert "already registered" in error
    
    def test_register_admin_role(self, db_session):
        """Should be able to register user with admin role."""
        user, error = register_user(
            email="admin@test.auth.com",
            password="admin_password",
            full_name="Admin User",
            user_type="lecturer",
            role=RoleEnum.admin
        )
        
        assert error is None
        assert user.role == RoleEnum.admin


# ============================================================================
# User Authentication Tests
# ============================================================================

class TestUserAuthentication:
    """Tests for user authentication."""
    
    def test_authenticate_valid_student(self, db_session):
        """Should authenticate student with correct credentials."""
        # Register a student first
        register_user(
            email="auth_student@test.auth.com",
            password="correct_password",
            full_name="Auth Student",
            user_type="student"
        )
        
        # Authenticate
        user, user_type = authenticate_user("auth_student@test.auth.com", "correct_password")
        
        assert user is not None
        assert user_type == "student"
        assert user.email == "auth_student@test.auth.com"
    
    def test_authenticate_valid_lecturer(self, db_session):
        """Should authenticate lecturer with correct credentials."""
        # Register a lecturer first
        register_user(
            email="auth_lecturer@test.auth.com",
            password="lecturer_pass",
            full_name="Auth Lecturer",
            user_type="lecturer"
        )
        
        # Authenticate
        user, user_type = authenticate_user("auth_lecturer@test.auth.com", "lecturer_pass")
        
        assert user is not None
        assert user_type == "lecturer"
    
    def test_authenticate_wrong_password(self, db_session):
        """Should fail with incorrect password."""
        # Register user
        register_user(
            email="wrong_pass@test.auth.com",
            password="correct_password",
            full_name="Test User",
            user_type="student"
        )
        
        # Try wrong password
        user, user_type = authenticate_user("wrong_pass@test.auth.com", "wrong_password")
        
        assert user is None
        assert user_type is None
    
    def test_authenticate_nonexistent_email(self, db_session):
        """Should fail with non-existent email."""
        user, user_type = authenticate_user("nonexistent@test.auth.com", "any_password")
        
        assert user is None
        assert user_type is None


# ============================================================================
# API Endpoint Tests
# ============================================================================

class TestAuthEndpoints:
    """Tests for authentication API endpoints."""
    
    def test_register_endpoint_success(self, client, db_session):
        """POST /api/auth/register should create new user."""
        response = client.post('/api/auth/register', json={
            'email': 'api_register@test.auth.com',
            'password': 'password123',
            'full_name': 'API Test User',
            'user_type': 'student'
        })
        
        assert response.status_code == 201
        data = response.get_json()
        assert 'token' in data
        assert data['user']['email'] == 'api_register@test.auth.com'
    
    def test_register_endpoint_missing_fields(self, client):
        """POST /api/auth/register should fail with missing fields."""
        response = client.post('/api/auth/register', json={
            'email': 'incomplete@test.auth.com'
            # Missing password and full_name
        })
        
        assert response.status_code == 400
        assert 'Missing required field' in response.get_json()['message']
    
    def test_login_endpoint_success(self, client, db_session):
        """POST /api/auth/login should return token for valid credentials."""
        # First register
        client.post('/api/auth/register', json={
            'email': 'login_test@test.auth.com',
            'password': 'password123',
            'full_name': 'Login Test User',
            'user_type': 'student'
        })
        
        # Then login
        response = client.post('/api/auth/login', json={
            'email': 'login_test@test.auth.com',
            'password': 'password123'
        })
        
        assert response.status_code == 200
        data = response.get_json()
        assert 'token' in data
        assert data['message'] == 'Login successful'
    
    def test_login_endpoint_invalid_credentials(self, client):
        """POST /api/auth/login should fail with invalid credentials."""
        response = client.post('/api/auth/login', json={
            'email': 'nonexistent@test.auth.com',
            'password': 'wrongpassword'
        })
        
        assert response.status_code == 401
        assert 'Invalid email or password' in response.get_json()['message']
    
    def test_me_endpoint_with_valid_token(self, client, db_session):
        """GET /api/auth/me should return user info with valid token."""
        # Register and get token
        register_response = client.post('/api/auth/register', json={
            'email': 'me_test@test.auth.com',
            'password': 'password123',
            'full_name': 'Me Test User',
            'user_type': 'student'
        })
        token = register_response.get_json()['token']
        
        # Call /me endpoint
        response = client.get('/api/auth/me', headers={
            'Authorization': f'Bearer {token}'
        })
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['user']['email'] == 'me_test@test.auth.com'
    
    def test_me_endpoint_without_token(self, client):
        """GET /api/auth/me should fail without token."""
        response = client.get('/api/auth/me')
        
        assert response.status_code == 401
        assert 'Missing authorization token' in response.get_json()['message']


# ============================================================================
# Protected Endpoint Tests
# ============================================================================

class TestProtectedEndpoints:
    """Tests for accessing protected endpoints with authorization."""
    
    def test_access_without_token_returns_401(self, client):
        """Accessing protected endpoint without token should return 401."""
        # This test will be relevant once we protect endpoints
        # For now, we test the /auth/me endpoint which requires auth
        response = client.get('/api/auth/me')
        assert response.status_code == 401
    
    def test_access_with_invalid_token_returns_401(self, client):
        """Accessing with invalid token should return 401."""
        response = client.get('/api/auth/me', headers={
            'Authorization': 'Bearer invalid.token.here'
        })
        assert response.status_code == 401
