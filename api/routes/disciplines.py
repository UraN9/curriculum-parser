"""Disciplines API endpoints."""
from flask_restful import Resource, reqparse
from flask import request
from marshmallow import ValidationError
from app.database import SessionLocal
from app.models import Discipline, Lecturer
from api.validations import DisciplineSchema


# Initialize schema
discipline_schema = DisciplineSchema()


def get_db():
    """Get database session."""
    return SessionLocal()


def validate_lecturer_exists(db, lecturer_id):
    """
    Validate that lecturer exists in database (relation validation).
    
    Args:
        db: Database session
        lecturer_id: ID to check
        
    Returns:
        tuple: (is_valid, error_message)
    """
    lecturer = db.query(Lecturer).filter(Lecturer.id == lecturer_id).first()
    if not lecturer:
        return False, f"Lecturer with ID {lecturer_id} does not exist"
    return True, None


class DisciplineListResource(Resource):
    """Handle /api/disciplines endpoint."""
    
    def get(self):
        """Get all disciplines."""
        db = get_db()
        try:
            disciplines = db.query(Discipline).all()
            return [{
                'id': d.id,
                'name': d.name,
                'course': d.course,
                'ects_credits': float(d.ects_credits),
                'lecturer_id': d.lecturer_id
            } for d in disciplines]
        finally:
            db.close()
    
    def post(self):
        """
        Create new discipline.
        
        Returns:
            201: Created successfully
            400: Missing data
            422: Validation error
            500: Server error
        """
        db = get_db()
        try:
            data = request.get_json()
            
            if not data:
                return {'error': 'No data provided'}, 400
            
            # Validate data using schema (returns 422 on error)
            try:
                validated_data = discipline_schema.load(data)
            except ValidationError as err:
                return {'error': 'Validation failed', 'details': err.messages}, 422
            
            # Validate relation: lecturer must exist
            is_valid, error_msg = validate_lecturer_exists(db, data['lecturer_id'])
            if not is_valid:
                return {'error': 'Validation failed', 'details': {'lecturer_id': [error_msg]}}, 422
            
            # Create new discipline
            discipline = Discipline(
                name=data['name'],
                course=data['course'],
                ects_credits=data['ects_credits'],
                lecturer_id=data['lecturer_id']
            )
            
            db.add(discipline)
            db.commit()
            db.refresh(discipline)
            
            return {
                'id': discipline.id,
                'name': discipline.name,
                'course': discipline.course,
                'ects_credits': float(discipline.ects_credits),
                'lecturer_id': discipline.lecturer_id
            }, 201
        except Exception as e:
            db.rollback()
            return {'error': str(e)}, 500
        finally:
            db.close()


class DisciplineResource(Resource):
    """Handle /api/disciplines/<id> endpoint."""
    
    def get(self, id):
        """Get discipline by ID."""
        db = get_db()
        try:
            discipline = db.query(Discipline).filter(Discipline.id == id).first()
            
            if not discipline:
                return {'error': 'Discipline not found'}, 404
            
            return {
                'id': discipline.id,
                'name': discipline.name,
                'course': discipline.course,
                'ects_credits': float(discipline.ects_credits),
                'lecturer_id': discipline.lecturer_id
            }
        finally:
            db.close()
    
    def put(self, id):
        """
        Update discipline by ID.
        
        Returns:
            200: Updated successfully
            400: Missing data
            404: Not found
            422: Validation error
            500: Server error
        """
        db = get_db()
        try:
            discipline = db.query(Discipline).filter(Discipline.id == id).first()
            
            if not discipline:
                return {'error': 'Discipline not found'}, 404
            
            data = request.get_json()
            if not data:
                return {'error': 'No data provided'}, 400
            
            # Validate data using schema (partial=True for partial updates)
            try:
                validated_data = discipline_schema.load(data, partial=True)
            except ValidationError as err:
                return {'error': 'Validation failed', 'details': err.messages}, 422
            
            # Validate relation if lecturer_id is being updated
            if 'lecturer_id' in data:
                is_valid, error_msg = validate_lecturer_exists(db, data['lecturer_id'])
                if not is_valid:
                    return {'error': 'Validation failed', 'details': {'lecturer_id': [error_msg]}}, 422
            
            # Update fields if provided
            if 'name' in data:
                discipline.name = data['name']
            if 'course' in data:
                discipline.course = data['course']
            if 'ects_credits' in data:
                discipline.ects_credits = data['ects_credits']
            if 'lecturer_id' in data:
                discipline.lecturer_id = data['lecturer_id']
            
            db.commit()
            db.refresh(discipline)
            
            return {
                'id': discipline.id,
                'name': discipline.name,
                'course': discipline.course,
                'ects_credits': float(discipline.ects_credits),
                'lecturer_id': discipline.lecturer_id
            }
        except Exception as e:
            db.rollback()
            return {'error': str(e)}, 500
        finally:
            db.close()
    
    def delete(self, id):
        """Delete discipline by ID."""
        db = get_db()
        try:
            discipline = db.query(Discipline).filter(Discipline.id == id).first()
            
            if not discipline:
                return {'error': 'Discipline not found'}, 404
            
            db.delete(discipline)
            db.commit()
            
            return {'message': f'Discipline {id} deleted successfully'}
        except Exception as e:
            db.rollback()
            return {'error': str(e)}, 500
        finally:
            db.close()
