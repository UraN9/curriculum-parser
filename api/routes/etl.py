"""
ETL API Routes

Provides endpoints for managing asynchronous ETL jobs:
- POST /api/etl/start - Start new ETL job
- GET /api/etl/status/<task_id> - Get job status
- GET /api/etl/jobs - List all jobs (with pagination)
- GET /api/etl/jobs/<job_id> - Get job details
"""

from flask import Blueprint, request, g
from flask_restful import Api, Resource
from datetime import datetime

from app.database import SessionLocal
from app.models import ETLJob, ETLJobStatus, Discipline


etl_bp = Blueprint('etl', __name__)
api = Api(etl_bp)


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


class ETLStart(Resource):
    """Start a new ETL job asynchronously."""
    
    def post(self):
        """
        Start ETL process asynchronously.
        
        Request body:
        {
            "input_file": "path/to/file.xlsx",
            "discipline_id": 1
        }
        
        Returns:
        {
            "message": "ETL job started",
            "task_id": "abc-123-def",
            "status_url": "/api/etl/status/abc-123-def"
        }
        """
        current_user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        data = request.get_json()
        
        if not data:
            return {"error": "No data provided"}, 400
        
        input_file = data.get('input_file')
        discipline_id = data.get('discipline_id')
        
        if not input_file:
            return {"error": "input_file is required"}, 400
        
        if not discipline_id:
            return {"error": "discipline_id is required"}, 400
        
        # Verify discipline exists
        db = SessionLocal()
        try:
            discipline = db.query(Discipline).filter_by(id=discipline_id).first()
            if not discipline:
                return {"error": f"Discipline with id={discipline_id} not found"}, 404
        finally:
            db.close()
        
        # Import and start Celery task
        try:
            from celery_app.tasks import run_etl_task
            
            # Start async task
            task = run_etl_task.delay(
                input_file=input_file,
                discipline_id=discipline_id,
                user_id=current_user.get('user_id')
            )
            
            return {
                "message": "ETL job started",
                "task_id": task.id,
                "status_url": f"/api/etl/status/{task.id}"
            }, 202
            
        except Exception as e:
            return {"error": f"Failed to start ETL job: {str(e)}"}, 500


class ETLStatus(Resource):
    """Get status of an ETL job."""
    
    def get(self, task_id):
        """
        Get ETL job status by task ID.
        
        Returns:
        {
            "task_id": "abc-123-def",
            "status": "completed",
            "progress": 100,
            "result": {...}
        }
        """
        current_user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        # Try to get from Celery first
        try:
            from celery_app.tasks import run_etl_task
            from celery.result import AsyncResult
            
            result = AsyncResult(task_id, app=run_etl_task.app)
            
            response = {
                "task_id": task_id,
                "status": result.status,
            }
            
            if result.status == 'PENDING':
                response["message"] = "Task is waiting to be processed"
                response["progress"] = 0
            elif result.status == 'STARTED':
                response["message"] = "Task has started"
                response["progress"] = 10
            elif result.status == 'PROGRESS':
                meta = result.info or {}
                response["message"] = meta.get('status', 'Processing')
                response["progress"] = meta.get('progress', 50)
            elif result.status == 'SUCCESS':
                response["message"] = "Task completed successfully"
                response["progress"] = 100
                response["result"] = result.result
            elif result.status == 'FAILURE':
                response["message"] = "Task failed"
                response["progress"] = 0
                response["error"] = str(result.result)
            
            return response, 200
            
        except Exception as e:
            # Fallback to database
            pass
        
        # Try to get from database
        db = SessionLocal()
        try:
            job = db.query(ETLJob).filter_by(task_id=task_id).first()
            
            if not job:
                return {"error": "Job not found"}, 404
            
            response = {
                "task_id": task_id,
                "status": job.status.value if job.status else "unknown",
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            }
            
            if job.status == ETLJobStatus.COMPLETED:
                response["progress"] = 100
                response["statistics"] = {
                    "records_processed": job.records_processed,
                    "records_created": job.records_created,
                    "records_updated": job.records_updated,
                    "records_skipped": job.records_skipped
                }
            elif job.status == ETLJobStatus.FAILED:
                response["progress"] = 0
                response["error"] = job.error_message
            else:
                response["progress"] = 50  # Running
            
            return response, 200
            
        finally:
            db.close()


class ETLJobList(Resource):
    """List ETL jobs with pagination."""
    
    def get(self):
        """
        List all ETL jobs.
        
        Query params:
        - page: Page number (default: 1)
        - per_page: Items per page (default: 10, max: 100)
        - status: Filter by status (optional)
        
        Returns:
        {
            "jobs": [...],
            "total": 50,
            "page": 1,
            "per_page": 10,
            "pages": 5
        }
        """
        current_user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 10, type=int), 100)
        status_filter = request.args.get('status')
        
        db = SessionLocal()
        try:
            query = db.query(ETLJob).order_by(ETLJob.created_at.desc())
            
            # Apply status filter if provided
            if status_filter:
                try:
                    status_enum = ETLJobStatus(status_filter)
                    query = query.filter(ETLJob.status == status_enum)
                except ValueError:
                    return {"error": f"Invalid status: {status_filter}"}, 400
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            jobs = query.offset((page - 1) * per_page).limit(per_page).all()
            
            return {
                "jobs": [
                    {
                        "id": job.id,
                        "task_id": job.task_id,
                        "input_file": job.input_file,
                        "discipline_id": job.discipline_id,
                        "status": job.status.value if job.status else None,
                        "created_at": job.created_at.isoformat() if job.created_at else None,
                        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                        "records_processed": job.records_processed,
                        "duration_seconds": job.duration_seconds
                    }
                    for job in jobs
                ],
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": (total + per_page - 1) // per_page
            }, 200
            
        finally:
            db.close()


class ETLJobDetail(Resource):
    """Get detailed information about a specific ETL job."""
    
    def get(self, job_id):
        """
        Get ETL job details by ID.
        
        Returns full job information including result summary and error details.
        """
        current_user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        db = SessionLocal()
        try:
            job = db.query(ETLJob).filter_by(id=job_id).first()
            
            if not job:
                return {"error": "Job not found"}, 404
            
            return {
                "id": job.id,
                "task_id": job.task_id,
                "input_file": job.input_file,
                "discipline_id": job.discipline_id,
                "user_id": job.user_id,
                "status": job.status.value if job.status else None,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "duration_seconds": job.duration_seconds,
                "statistics": {
                    "records_processed": job.records_processed,
                    "records_created": job.records_created,
                    "records_updated": job.records_updated,
                    "records_skipped": job.records_skipped
                },
                "result_summary": job.result_summary,
                "error_message": job.error_message
            }, 200
            
        finally:
            db.close()


class ETLHealth(Resource):
    """Check ETL service health."""
    
    def get(self):
        """
        Check if Celery and Redis are available.
        
        Returns:
        {
            "status": "healthy",
            "celery": "connected",
            "redis": "connected"
        }
        """
        response = {
            "status": "healthy",
            "celery": "unknown",
            "redis": "unknown"
        }
        
        try:
            from celery_app.tasks import check_etl_health
            
            # Try to ping Celery
            result = check_etl_health.delay()
            health = result.get(timeout=5)
            
            response["celery"] = "connected"
            response["redis"] = "connected"
            
        except Exception as e:
            response["status"] = "degraded"
            response["celery"] = "disconnected"
            response["redis"] = "disconnected"
            response["error"] = str(e)
        
        status_code = 200 if response["status"] == "healthy" else 503
        return response, status_code


# Register routes
api.add_resource(ETLStart, '/start')
api.add_resource(ETLStatus, '/status/<string:task_id>')
api.add_resource(ETLJobList, '/jobs')
api.add_resource(ETLJobDetail, '/jobs/<int:job_id>')
api.add_resource(ETLHealth, '/health')
