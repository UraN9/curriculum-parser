"""
Topics (Themes) API Routes

Provides read-only endpoints for topics/themes:
- GET /api/topics - List all topics
- GET /api/topics/<id> - Get topic details
"""

from flask import Blueprint, request
from flask_restful import Api, Resource

from app.database import SessionLocal
from app.models import Theme, Section, Discipline


topics_bp = Blueprint('topics', __name__)
api = Api(topics_bp)


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


class TopicList(Resource):
    """List all topics/themes."""
    
    def get(self):
        """
        Get list of all topics with pagination.
        
        Query params:
        - page: Page number (default: 1)
        - per_page: Items per page (default: 20, max: 100)
        - section_id: Filter by section
        - discipline_id: Filter by discipline
        
        Returns:
        {
            "topics": [...],
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
        section_id = request.args.get('section_id', type=int)
        discipline_id = request.args.get('discipline_id', type=int)
        
        session = SessionLocal()
        try:
            query = session.query(Theme)
            
            # Apply filters
            if section_id:
                query = query.filter(Theme.section_id == section_id)
            
            if discipline_id:
                query = query.join(Section).filter(Section.discipline_id == discipline_id)
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            themes = query.order_by(Theme.id).offset((page - 1) * per_page).limit(per_page).all()
            
            # Build response
            topics = []
            for theme in themes:
                section = session.query(Section).filter_by(id=theme.section_id).first()
                topics.append({
                    "id": theme.id,
                    "name": theme.name,
                    "total_hours": theme.total_hours,
                    "section_id": theme.section_id,
                    "section_name": section.name if section else None,
                    "discipline_id": section.discipline_id if section else None
                })
            
            return {
                "topics": topics,
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            }, 200
            
        finally:
            session.close()


class TopicDetail(Resource):
    """Get topic/theme details."""
    
    def get(self, topic_id):
        """
        Get topic details by ID.
        
        Returns:
        {
            "id": 1,
            "name": "Topic name",
            "total_hours": 10,
            "section": {...},
            "activities": [...]
        }
        """
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        session = SessionLocal()
        try:
            theme = session.query(Theme).filter_by(id=topic_id).first()
            
            if not theme:
                return {
                    "error": "Not Found",
                    "message": f"Topic with id={topic_id} not found"
                }, 404
            
            # Get section info
            section = session.query(Section).filter_by(id=theme.section_id).first()
            
            # Get activities for this theme
            from app.models import Activity, ActivityType
            activities = session.query(Activity).filter_by(theme_id=theme.id).all()
            
            activities_list = []
            for act in activities:
                act_type = session.query(ActivityType).filter_by(id=act.type_id).first()
                activities_list.append({
                    "id": act.id,
                    "name": act.name,
                    "type": act_type.name if act_type else None,
                    "hours": act.hours
                })
            
            return {
                "id": theme.id,
                "name": theme.name,
                "total_hours": theme.total_hours,
                "section": {
                    "id": section.id,
                    "name": section.name,
                    "discipline_id": section.discipline_id
                } if section else None,
                "activities": activities_list,
                "activities_count": len(activities_list)
            }, 200
            
        finally:
            session.close()


# Register resources
api.add_resource(TopicList, '')
api.add_resource(TopicDetail, '/<int:topic_id>')
