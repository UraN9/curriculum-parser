"""
Authentication and Authorization module for Flask API.

Provides JWT-based authentication with SHA256 password hashing.
Supports two-level authorization: application level (decorators) and database level (RLS).
"""

import jwt
import hashlib
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import request, jsonify, g
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Lecturer, Student, RoleEnum
from api.config import Config


# ============================================================================
# Password Hashing (SHA256)
# ============================================================================

def hash_password(password: str) -> str:
    """
    Hash a password using SHA256.
    
    Args:
        password: Plain text password
        
    Returns:
        SHA256 hashed password as hex string
    """
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    """
    Verify a password against its hash.
    
    Args:
        password: Plain text password to verify
        password_hash: Stored SHA256 hash
        
    Returns:
        True if password matches, False otherwise
    """
    return hash_password(password) == password_hash


# ============================================================================
# JWT Token Management
# ============================================================================

def generate_token(user_id: int, email: str, role: str, user_type: str) -> str:
    """
    Generate a JWT token for authenticated user.
    
    Args:
        user_id: Database ID of the user
        email: User's email address
        role: User's role (admin, lecturer, viewer)
        user_type: Type of user ('lecturer' or 'student')
        
    Returns:
        Encoded JWT token string
    """
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'user_type': user_type,
        'exp': datetime.now(timezone.utc) + timedelta(hours=Config.JWT_EXPIRATION_HOURS),
        'iat': datetime.now(timezone.utc)
    }
    
    token = jwt.encode(payload, Config.JWT_SECRET_KEY, algorithm='HS256')
    return token


def decode_token(token: str) -> dict:
    """
    Decode and verify a JWT token.
    
    Args:
        token: JWT token string
        
    Returns:
        Decoded payload dictionary
        
    Raises:
        jwt.ExpiredSignatureError: If token has expired
        jwt.InvalidTokenError: If token is invalid
    """
    return jwt.decode(token, Config.JWT_SECRET_KEY, algorithms=['HS256'])


# ============================================================================
# Authentication Decorators
# ============================================================================

def token_required(f):
    """
    Decorator that requires a valid JWT token for the endpoint.
    
    Extracts token from Authorization header (Bearer <token>).
    Sets g.current_user with user info if valid.
    
    Returns 401 if token is missing or invalid.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Extract token from Authorization header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        
        if not token:
            return jsonify({
                'error': 'Authentication required',
                'message': 'Missing authorization token'
            }), 401
        
        try:
            # Decode and verify token
            payload = decode_token(token)
            
            # Store user info in Flask's g object
            g.current_user = {
                'user_id': payload['user_id'],
                'email': payload['email'],
                'role': payload['role'],
                'user_type': payload['user_type']
            }
            
            # Set database session variables for RLS
            _set_db_session_context(payload['user_id'], payload['role'])
            
        except jwt.ExpiredSignatureError:
            return jsonify({
                'error': 'Token expired',
                'message': 'Please login again'
            }), 401
        except jwt.InvalidTokenError:
            return jsonify({
                'error': 'Invalid token',
                'message': 'Token verification failed'
            }), 401
        
        return f(*args, **kwargs)
    
    return decorated


def role_required(*allowed_roles):
    """
    Decorator that requires specific role(s) for the endpoint.
    
    Must be used after @token_required decorator.
    
    Args:
        allowed_roles: Role names that are allowed to access (e.g., 'admin', 'lecturer')
        
    Returns 403 if user's role is not in allowed_roles.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not hasattr(g, 'current_user'):
                return jsonify({
                    'error': 'Authentication required',
                    'message': 'Please authenticate first'
                }), 401
            
            user_role = g.current_user.get('role')
            
            # Admin has access to everything
            if user_role == 'admin':
                return f(*args, **kwargs)
            
            # Check if user's role is in allowed roles
            if user_role not in allowed_roles:
                return jsonify({
                    'error': 'Access denied',
                    'message': f'Required role: {", ".join(allowed_roles)}'
                }), 403
            
            return f(*args, **kwargs)
        
        return decorated
    return decorator


def admin_required(f):
    """
    Decorator that requires admin role for the endpoint.
    
    Shortcut for @role_required('admin').
    Must be used after @token_required decorator.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not hasattr(g, 'current_user'):
            return jsonify({
                'error': 'Authentication required',
                'message': 'Please authenticate first'
            }), 401
        
        if g.current_user.get('role') != 'admin':
            return jsonify({
                'error': 'Access denied',
                'message': 'Admin privileges required'
            }), 403
        
        return f(*args, **kwargs)
    
    return decorated


# ============================================================================
# Database Session Context (for RLS)
# ============================================================================

def _set_db_session_context(user_id: int, role: str):
    """
    Set PostgreSQL session variables for Row-Level Security.
    
    These variables are used by RLS policies to filter data
    based on the current user.
    
    Args:
        user_id: Current user's database ID
        role: Current user's role
    """
    # This will be called before each request that requires auth
    # The actual SET commands will be executed in the database session
    g.db_context = {
        'user_id': user_id,
        'role': role
    }


def apply_db_context(db_session: Session):
    """
    Apply the current user context to a database session.
    
    Call this function when you need RLS to be applied.
    
    Args:
        db_session: SQLAlchemy session
    """
    if hasattr(g, 'db_context'):
        user_id = g.db_context['user_id']
        role = g.db_context['role']
        
        # Set PostgreSQL session variables
        db_session.execute(f"SET app.current_user_id = '{user_id}'")
        db_session.execute(f"SET app.current_role = '{role}'")


# ============================================================================
# User Authentication Functions
# ============================================================================

def authenticate_user(email: str, password: str) -> tuple:
    """
    Authenticate a user (lecturer or student) by email and password.
    
    Args:
        email: User's email address
        password: Plain text password
        
    Returns:
        Tuple of (user_object, user_type) if authenticated, (None, None) otherwise
    """
    db = SessionLocal()
    try:
        # Check lecturers first
        lecturer = db.query(Lecturer).filter(Lecturer.email == email).first()
        if lecturer and verify_password(password, lecturer.password_hash):
            return lecturer, 'lecturer'
        
        # Check students
        student = db.query(Student).filter(Student.email == email).first()
        if student and verify_password(password, student.password_hash):
            return student, 'student'
        
        return None, None
    finally:
        db.close()


def register_user(
    email: str,
    password: str,
    full_name: str,
    user_type: str = 'student',
    role: RoleEnum = RoleEnum.viewer
) -> tuple:
    """
    Register a new user (lecturer or student).
    
    Args:
        email: User's email address
        password: Plain text password (will be hashed)
        full_name: User's full name
        user_type: 'lecturer' or 'student'
        role: User's role (default: viewer)
        
    Returns:
        Tuple of (user_object, error_message)
        If successful: (user, None)
        If failed: (None, error_message)
    """
    db = SessionLocal()
    try:
        # Check if email already exists
        existing_lecturer = db.query(Lecturer).filter(Lecturer.email == email).first()
        existing_student = db.query(Student).filter(Student.email == email).first()
        
        if existing_lecturer or existing_student:
            return None, 'Email already registered'
        
        # Hash password
        password_hash = hash_password(password)
        
        # Create user based on type
        if user_type == 'lecturer':
            user = Lecturer(
                email=email,
                password_hash=password_hash,
                full_name=full_name,
                role=role
            )
        else:
            user = Student(
                email=email,
                password_hash=password_hash,
                full_name=full_name,
                role=role
            )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        return user, None
    except Exception as e:
        db.rollback()
        return None, str(e)
    finally:
        db.close()
