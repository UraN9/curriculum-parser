"""
Schedule (Activities) API Routes

Provides read-only endpoints for schedule/activities:
- GET /api/schedule - List all activities
- GET /api/schedule/<id> - Get activity details
"""

from flask import Blueprint, request
from flask_restful import Api, Resource

from app.database import SessionLocal
from app.models import Activity, ActivityType, Theme, Section, Discipline


schedule_bp = Blueprint('schedule', __name__)
api = Api(schedule_bp)


def get_current_user():
    """
    Get current user from token.
    
    Returns tuple (user_dict, error_response, status_code)
    """
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None, {"error": "Authentication required", "message": "Missing authorization token"}, 401
    
    token = auth_header.split(' ')[1]
    
    try:
        from api.auth import decode_token
        payload = decode_token(token)
        return {
            'user_id': payload['user_id'],
            'email': payload['email'],
            'role': payload['role'],
            'user_type': payload['user_type']
        }, None, None
    except Exception as e:
        return None, {"error": "Invalid token", "message": str(e)}, 401


class ScheduleList(Resource):
    """List all activities (schedule)."""
    
    def get(self):
        """
        Get list of all activities with pagination.
        
        Query params:
        - page: Page number (default: 1)
        - per_page: Items per page (default: 20, max: 100)
        - theme_id: Filter by theme
        - type_id: Filter by activity type
        - discipline_id: Filter by discipline
        
        Returns:
        {
            "schedule": [...],
            "total": 100,
            "page": 1,
            "per_page": 20,
            "pages": 5
        }
        """
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        # Pagination params
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        
        # Filter params
        theme_id = request.args.get('theme_id', type=int)
        type_id = request.args.get('type_id', type=int)
        discipline_id = request.args.get('discipline_id', type=int)
        
        session = SessionLocal()
        try:
            query = session.query(Activity)
            
            # Apply filters
            if theme_id:
                query = query.filter(Activity.theme_id == theme_id)
            
            if type_id:
                query = query.filter(Activity.type_id == type_id)
            
            if discipline_id:
                query = query.join(Theme).join(Section).filter(Section.discipline_id == discipline_id)
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            activities = query.order_by(Activity.id).offset((page - 1) * per_page).limit(per_page).all()
            
            # Build response
            schedule = []
            for act in activities:
                act_type = session.query(ActivityType).filter_by(id=act.type_id).first()
                theme = session.query(Theme).filter_by(id=act.theme_id).first()
                
                schedule.append({
                    "id": act.id,
                    "name": act.name,
                    "hours": act.hours,
                    "type_id": act.type_id,
                    "type_name": act_type.name if act_type else None,
                    "theme_id": act.theme_id,
                    "theme_name": theme.name if theme else None
                })
            
            return {
                "schedule": schedule,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            }, 200
            
        finally:
            session.close()


class ScheduleDetail(Resource):
    """Get activity details."""
    
    def get(self, activity_id):
        """
        Get activity details by ID.
        
        Returns:
        {
            "id": 1,
            "name": "Activity name",
            "hours": 2,
            "type": {...},
            "theme": {...},
            "section": {...},
            "discipline": {...}
        }
        """
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        session = SessionLocal()
        try:
            activity = session.query(Activity).filter_by(id=activity_id).first()
            
            if not activity:
                return {
                    "error": "Not Found",
                    "message": f"Activity with id={activity_id} not found"
                }, 404
            
            # Get related data
            act_type = session.query(ActivityType).filter_by(id=activity.type_id).first()
            theme = session.query(Theme).filter_by(id=activity.theme_id).first()
            section = session.query(Section).filter_by(id=theme.section_id).first() if theme else None
            discipline = session.query(Discipline).filter_by(id=section.discipline_id).first() if section else None
            
            return {
                "id": activity.id,
                "name": activity.name,
                "hours": activity.hours,
                "type": {
                    "id": act_type.id,
                    "name": act_type.name
                } if act_type else None,
                "theme": {
                    "id": theme.id,
                    "name": theme.name,
                    "total_hours": theme.total_hours
                } if theme else None,
                "section": {
                    "id": section.id,
                    "name": section.name
                } if section else None,
                "discipline": {
                    "id": discipline.id,
                    "name": discipline.name,
                    "course": discipline.course
                } if discipline else None
            }, 200
            
        finally:
            session.close()


class ActivityTypesList(Resource):
    """List all activity types."""
    
    def get(self):
        """
        Get list of all activity types.
        
        Returns:
        {
            "types": [
                {"id": 1, "name": "Лекція"},
                {"id": 2, "name": "Практичне заняття"},
                ...
            ]
        }
        """
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        session = SessionLocal()
        try:
            types = session.query(ActivityType).order_by(ActivityType.id).all()
            
            return {
                "types": [{"id": t.id, "name": t.name} for t in types]
            }, 200
            
        finally:
            session.close()


# Register resources
api.add_resource(ScheduleList, '')
api.add_resource(ScheduleDetail, '/<int:activity_id>')
api.add_resource(ActivityTypesList, '/types')
