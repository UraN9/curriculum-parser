"""
Authentication Routes for Flask API.

Provides endpoints for user registration and login.
"""

from flask import request, jsonify
from flask_restful import Resource

from api.auth import (
    authenticate_user,
    register_user,
    generate_token,
    hash_password
)
from app.models import RoleEnum


class AuthRegister(Resource):
    """
    User Registration endpoint.
    
    POST /api/auth/register
    """
    
    def post(self):
        """
        Register a new user (lecturer or student).
        
        Request body:
        {
            "email": "user@example.com",
            "password": "password123",
            "full_name": "John Doe",
            "user_type": "student",  // or "lecturer"
            "role": "viewer"  // optional, default: viewer
        }
        
        Returns:
            201: User created successfully with JWT token
            400: Invalid request data
            409: Email already registered
        """
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['email', 'password', 'full_name']
        for field in required_fields:
            if not data.get(field):
                return {
                    'error': 'Validation error',
                    'message': f'Missing required field: {field}'
                }, 400
        
        # Validate email format
        email = data['email'].strip().lower()
        if '@' not in email or '.' not in email:
            return {
                'error': 'Validation error',
                'message': 'Invalid email format'
            }, 400
        
        # Validate password length
        password = data['password']
        if len(password) < 6:
            return {
                'error': 'Validation error',
                'message': 'Password must be at least 6 characters'
            }, 400
        
        # Get optional fields
        user_type = data.get('user_type', 'student')
        if user_type not in ('student', 'lecturer'):
            return {
                'error': 'Validation error',
                'message': 'user_type must be "student" or "lecturer"'
            }, 400
        
        # Parse role (default: viewer for students, lecturer for lecturers)
        role_str = data.get('role', 'viewer' if user_type == 'student' else 'lecturer')
        try:
            role = RoleEnum(role_str)
        except ValueError:
            return {
                'error': 'Validation error',
                'message': f'Invalid role. Must be one of: {", ".join([r.value for r in RoleEnum])}'
            }, 400
        
        # Register user
        user, error = register_user(
            email=email,
            password=password,
            full_name=data['full_name'].strip(),
            user_type=user_type,
            role=role
        )
        
        if error:
            if 'already registered' in error:
                return {
                    'error': 'Conflict',
                    'message': error
                }, 409
            return {
                'error': 'Registration failed',
                'message': error
            }, 400
        
        # Generate JWT token
        token = generate_token(
            user_id=user.id,
            email=user.email,
            role=user.role.value,
            user_type=user_type
        )
        
        return {
            'message': 'User registered successfully',
            'user': {
                'id': user.id,
                'email': user.email,
                'full_name': user.full_name,
                'role': user.role.value,
                'user_type': user_type
            },
            'token': token
        }, 201


class AuthLogin(Resource):
    """
    User Login endpoint.
    
    POST /api/auth/login
    """
    
    def post(self):
        """
        Authenticate user and return JWT token.
        
        Request body:
        {
            "email": "user@example.com",
            "password": "password123"
        }
        
        Returns:
            200: Login successful with JWT token
            400: Invalid request data
            401: Invalid credentials
        """
        data = request.get_json()
        
        # Validate required fields
        if not data.get('email') or not data.get('password'):
            return {
                'error': 'Validation error',
                'message': 'Email and password are required'
            }, 400
        
        email = data['email'].strip().lower()
        password = data['password']
        
        # Authenticate user
        user, user_type = authenticate_user(email, password)
        
        if not user:
            return {
                'error': 'Authentication failed',
                'message': 'Invalid email or password'
            }, 401
        
        # Generate JWT token
        token = generate_token(
            user_id=user.id,
            email=user.email,
            role=user.role.value,
            user_type=user_type
        )
        
        return {
            'message': 'Login successful',
            'user': {
                'id': user.id,
                'email': user.email,
                'full_name': user.full_name,
                'role': user.role.value,
                'user_type': user_type
            },
            'token': token
        }, 200


class AuthMe(Resource):
    """
    Get current user info endpoint.
    
    GET /api/auth/me
    """
    
    def get(self):
        """
        Get current authenticated user's information.
        
        Requires Authorization header with Bearer token.
        
        Returns:
            200: Current user info
            401: Not authenticated
        """
        from api.auth import token_required
        from flask import g
        
        # Manual token check (since we can't use decorator on Resource method easily)
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return {
                'error': 'Authentication required',
                'message': 'Missing authorization token'
            }, 401
        
        token = auth_header.split(' ')[1]
        
        try:
            from api.auth import decode_token
            payload = decode_token(token)
            
            return {
                'user': {
                    'id': payload['user_id'],
                    'email': payload['email'],
                    'role': payload['role'],
                    'user_type': payload['user_type']
                }
            }, 200
            
        except Exception as e:
            return {
                'error': 'Invalid token',
                'message': str(e)
            }, 401
