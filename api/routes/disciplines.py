"""Disciplines API endpoints."""
from flask_restful import Resource, reqparse
from flask import request
from app.database import SessionLocal
from app.models import Discipline


def get_db():
    """Get database session."""
    return SessionLocal()


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
        """Create new discipline."""
        db = get_db()
        try:
            data = request.get_json()
            
            # Validate required fields
            if not data:
                return {'error': 'No data provided'}, 400
            
            required = ['name', 'course', 'ects_credits', 'lecturer_id']
            for field in required:
                if field not in data:
                    return {'error': f'Missing required field: {field}'}, 400
            
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
        """Update discipline by ID."""
        db = get_db()
        try:
            discipline = db.query(Discipline).filter(Discipline.id == id).first()
            
            if not discipline:
                return {'error': 'Discipline not found'}, 404
            
            data = request.get_json()
            if not data:
                return {'error': 'No data provided'}, 400
            
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
