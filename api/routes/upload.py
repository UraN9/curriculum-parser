"""
File Upload API Routes

Provides endpoint for uploading Excel files and auto-starting ETL:
- POST /api/upload - Upload file and start ETL
"""

import os
import uuid
from datetime import datetime
from flask import Blueprint, request
from flask_restful import Api, Resource
from werkzeug.utils import secure_filename

from app.database import SessionLocal
from app.models import ETLJob, ETLJobStatus, Discipline


upload_bp = Blueprint('upload', __name__)
api = Api(upload_bp)

# Configuration
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'uploads')
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


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


class FileUpload(Resource):
    """Upload Excel file and automatically start ETL process."""
    
    def post(self):
        """
        Upload Excel file and start ETL.
        
        Form data:
        - file: Excel file (.xls or .xlsx)
        - discipline_id: ID of discipline (optional, default=1)
        
        Returns:
        {
            "message": "File uploaded successfully",
            "filename": "uploaded_file.xlsx",
            "task_id": "uuid",
            "job_id": 1,
            "status": "pending"
        }
        """
        # Check authentication
        user, error, status_code = get_current_user()
        if error:
            return error, status_code
        
        # Check if file is present
        if 'file' not in request.files:
            return {
                "error": "Bad Request",
                "message": "No file provided. Use 'file' field in multipart/form-data"
            }, 400
        
        file = request.files['file']
        
        # Check if filename is not empty
        if file.filename == '':
            return {
                "error": "Bad Request",
                "message": "No file selected"
            }, 400
        
        # Check file extension
        if not allowed_file(file.filename):
            return {
                "error": "Bad Request",
                "message": f"Invalid file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
            }, 400
        
        # Get discipline_id from form
        discipline_id = request.form.get('discipline_id', 1, type=int)
        
        # Verify discipline exists
        session = SessionLocal()
        try:
            discipline = session.query(Discipline).filter_by(id=discipline_id).first()
            if not discipline:
                return {
                    "error": "Not Found",
                    "message": f"Discipline with id={discipline_id} not found"
                }, 404
            
            # Create upload folder if not exists
            os.makedirs(UPLOAD_FOLDER, exist_ok=True)
            
            # Generate unique filename
            original_filename = secure_filename(file.filename)
            unique_id = uuid.uuid4().hex[:8]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{unique_id}_{original_filename}"
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            
            # Save file
            file.save(filepath)
            
            # Create ETL job
            task_id = str(uuid.uuid4())
            job = ETLJob(
                task_id=task_id,
                input_file=filepath,
                discipline_id=discipline_id,
                user_id=user['user_id'],
                status=ETLJobStatus.PENDING
            )
            session.add(job)
            session.commit()
            job_id = job.id
            
            # Try to start Celery task (async)
            try:
                from celery_app.tasks import run_etl_task
                run_etl_task.delay(job_id, filepath, discipline_id)
            except Exception as celery_error:
                # If Celery/Redis not available, run synchronously
                try:
                    from etl_service.etl import run_etl_pipeline
                    
                    # Update job status to RUNNING
                    job.status = ETLJobStatus.RUNNING
                    job.started_at = datetime.utcnow()
                    session.commit()
                    
                    # Run ETL
                    result = run_etl_pipeline(filepath, discipline_id=discipline_id)
                    
                    # Update job with results
                    job.status = ETLJobStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                    job.records_processed = result.get('records_processed', 0)
                    job.records_created = result.get('records_created', 0)
                    job.records_updated = result.get('records_updated', 0)
                    job.records_skipped = result.get('records_skipped', 0)
                    job.result_summary = str(result)
                    session.commit()
                    
                    return {
                        "message": "File uploaded and ETL completed (sync mode)",
                        "filename": original_filename,
                        "filepath": filepath,
                        "task_id": task_id,
                        "job_id": job_id,
                        "status": "completed",
                        "result": result
                    }, 201
                    
                except Exception as etl_error:
                    job.status = ETLJobStatus.FAILED
                    job.error_message = str(etl_error)
                    job.completed_at = datetime.utcnow()
                    session.commit()
                    
                    return {
                        "error": "ETL Failed",
                        "message": str(etl_error),
                        "filename": original_filename,
                        "task_id": task_id,
                        "job_id": job_id,
                        "status": "failed"
                    }, 500
            
            return {
                "message": "File uploaded successfully, ETL started",
                "filename": original_filename,
                "filepath": filepath,
                "task_id": task_id,
                "job_id": job_id,
                "status": "pending"
            }, 202
            
        finally:
            session.close()


# Register resources
api.add_resource(FileUpload, '')
