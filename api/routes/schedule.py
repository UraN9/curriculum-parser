"""
Schedule (Activities) API Routes

Provides read-only endpoints for schedule/activities:
- GET /api/schedule - List all activities
- GET /api/schedule/<id> - Get activity details
"""

from datetime import datetime, timedelta

from flask import Blueprint, request
from flask_restful import Api, Resource

from app.database import SessionLocal
from app.models import Activity, ActivityType, Theme, Section, Discipline, Schedule


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


WEEKDAY_TO_ENUM = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday",
}


def _parse_iso_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def _serialize_schedule_entry(entry, activity, theme, section, discipline, activity_type, date_value=None):
    payload = {
        "discipline_id": discipline.id if discipline else None,
        "discipline_name": discipline.name if discipline else None,
        "activity_id": activity.id,
        "activity_name": activity.name,
        "activity_hours": activity.hours,
        "theme_id": theme.id if theme else None,
        "theme_name": theme.name if theme else None,
        "section_id": section.id if section else None,
        "section_name": section.name if section else None,
        "type_id": activity_type.id if activity_type else None,
        "type_name": activity_type.name if activity_type else None,
        "weekday": entry.day.value if hasattr(entry.day, "value") else str(entry.day),
        "pair_number": entry.pair_number,
        "room": entry.room,
    }

    if date_value is not None:
        payload["date"] = date_value.isoformat()

    return payload


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


class SemesterScheduleBuilder(Resource):
    """Build schedule projection for a semester date interval."""

    def get(self):
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code

        start_date_raw = request.args.get("start_date")
        end_date_raw = request.args.get("end_date")
        discipline_id = request.args.get("discipline_id", type=int)

        if not start_date_raw or not end_date_raw:
            return {
                "error": "Validation Error",
                "message": "start_date and end_date are required in YYYY-MM-DD format",
            }, 400

        start_date = _parse_iso_date(start_date_raw)
        end_date = _parse_iso_date(end_date_raw)

        if not start_date or not end_date:
            return {
                "error": "Validation Error",
                "message": "Invalid date format. Use YYYY-MM-DD",
            }, 400

        if start_date > end_date:
            return {
                "error": "Validation Error",
                "message": "start_date must be before or equal to end_date",
            }, 400

        session = SessionLocal()
        try:
            if discipline_id:
                discipline = session.query(Discipline).filter_by(id=discipline_id).first()
                if not discipline:
                    return {
                        "error": "Not Found",
                        "message": f"Discipline with id={discipline_id} not found",
                    }, 404

            query = (
                session.query(Schedule, Activity, Theme, Section, Discipline, ActivityType)
                .join(Activity, Schedule.activity_id == Activity.id)
                .join(Theme, Activity.theme_id == Theme.id)
                .join(Section, Theme.section_id == Section.id)
                .join(Discipline, Section.discipline_id == Discipline.id)
                .outerjoin(ActivityType, Activity.type_id == ActivityType.id)
            )

            if discipline_id:
                query = query.filter(Discipline.id == discipline_id)

            base_entries = query.all()
            weekday_groups = {}
            for entry, activity, theme, section, discipline, activity_type in base_entries:
                weekday = entry.day.value if hasattr(entry.day, "value") else str(entry.day)
                weekday_groups.setdefault(weekday, []).append(
                    (entry, activity, theme, section, discipline, activity_type)
                )

            generated = []
            current = start_date
            while current <= end_date:
                weekday = WEEKDAY_TO_ENUM[current.weekday()]
                for item in weekday_groups.get(weekday, []):
                    generated.append(
                        _serialize_schedule_entry(*item, date_value=current)
                    )
                current += timedelta(days=1)

            generated.sort(key=lambda x: (x["date"], x["pair_number"], x["activity_id"]))

            return {
                "interval": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "discipline_id": discipline_id,
                },
                "generated_schedule": generated,
                "total": len(generated),
            }, 200
        finally:
            session.close()


class DisciplineScheduleByDate(Resource):
    """Get discipline schedule for a specific date."""

    def get(self, discipline_id, date_str):
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code

        target_date = _parse_iso_date(date_str)
        if not target_date:
            return {
                "error": "Validation Error",
                "message": "Invalid date format. Use YYYY-MM-DD",
            }, 400

        weekday = WEEKDAY_TO_ENUM[target_date.weekday()]

        session = SessionLocal()
        try:
            discipline = session.query(Discipline).filter_by(id=discipline_id).first()
            if not discipline:
                return {
                    "error": "Not Found",
                    "message": f"Discipline with id={discipline_id} not found",
                }, 404

            rows = (
                session.query(Schedule, Activity, Theme, Section, Discipline, ActivityType)
                .join(Activity, Schedule.activity_id == Activity.id)
                .join(Theme, Activity.theme_id == Theme.id)
                .join(Section, Theme.section_id == Section.id)
                .join(Discipline, Section.discipline_id == Discipline.id)
                .outerjoin(ActivityType, Activity.type_id == ActivityType.id)
                .filter(Discipline.id == discipline_id)
                .filter(Schedule.day == weekday)
                .all()
            )

            schedule_entries = [
                _serialize_schedule_entry(*row, date_value=target_date) for row in rows
            ]
            schedule_entries.sort(key=lambda x: (x["pair_number"], x["activity_id"]))

            return {
                "discipline_id": discipline_id,
                "date": target_date.isoformat(),
                "weekday": weekday,
                "schedule": schedule_entries,
                "total": len(schedule_entries),
            }, 200
        finally:
            session.close()


# Register resources
api.add_resource(ScheduleList, '')
api.add_resource(ScheduleDetail, '/<int:activity_id>')
api.add_resource(ActivityTypesList, '/types')
api.add_resource(SemesterScheduleBuilder, '/build')
api.add_resource(DisciplineScheduleByDate, '/discipline/<int:discipline_id>/date/<string:date_str>')
