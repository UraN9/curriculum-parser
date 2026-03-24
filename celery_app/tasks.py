"""
Celery Tasks for ETL Processing

Defines asynchronous tasks for running ETL operations.
"""

import os
import traceback
from datetime import datetime
from celery import current_task
from celery.exceptions import MaxRetriesExceededError

from .celery_config import celery_app
from app.database import SessionLocal
from app.models import ETLJob, ETLJobStatus


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def run_etl_task(self, input_file: str, discipline_id: int, user_id: int = None):
    """
    Run ETL process asynchronously.
    
    This task is idempotent - running it multiple times with the same
    parameters will not create duplicate data in the database.
    
    Args:
        input_file: Path to the Excel file to process
        discipline_id: ID of the discipline to associate data with
        user_id: Optional user ID who initiated the task
        
    Returns:
        dict: Result with status, message, and statistics
    """
    task_id = self.request.id
    db = SessionLocal()
    
    try:
        # Update task state to STARTED
        self.update_state(state='STARTED', meta={
            'status': 'Processing',
            'progress': 0,
            'started_at': datetime.utcnow().isoformat()
        })
        
        # Create or update ETL job record
        job = _get_or_create_job(db, task_id, input_file, discipline_id, user_id)
        job.status = ETLJobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()
        
        # Import ETL function here to avoid circular imports
        from etl_service.etl import run_etl_pipeline
        
        # Update progress
        self.update_state(state='PROGRESS', meta={
            'status': 'Extracting data',
            'progress': 25
        })
        
        # Run the ETL pipeline with idempotent flag
        result = run_etl_pipeline(
            input_file=input_file,
            discipline_id=discipline_id,
            idempotent=True  # Ensure no duplicates
        )
        
        # Update progress
        self.update_state(state='PROGRESS', meta={
            'status': 'Saving to database',
            'progress': 75
        })
        
        # Update job record with success
        job.status = ETLJobStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        job.result_summary = result.get('summary', {})
        job.records_processed = result.get('records_processed', 0)
        job.records_created = result.get('records_created', 0)
        job.records_updated = result.get('records_updated', 0)
        job.records_skipped = result.get('records_skipped', 0)
        db.commit()
        
        return {
            'status': 'SUCCESS',
            'task_id': task_id,
            'message': 'ETL completed successfully',
            'statistics': {
                'records_processed': job.records_processed,
                'records_created': job.records_created,
                'records_updated': job.records_updated,
                'records_skipped': job.records_skipped
            }
        }
        
    except FileNotFoundError as e:
        # Don't retry for file not found errors
        _handle_task_failure(db, task_id, str(e))
        raise
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        
        try:
            # Retry the task
            raise self.retry(exc=e)
        except MaxRetriesExceededError:
            # Max retries reached, mark as failed
            _handle_task_failure(db, task_id, error_msg)
            raise
            
    finally:
        db.close()


@celery_app.task
def check_etl_health():
    """
    Health check task for ETL service.
    
    Can be used to verify Celery is working correctly.
    
    Returns:
        dict: Health status
    """
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'celery-etl'
    }


def _get_or_create_job(db, task_id: str, input_file: str, 
                       discipline_id: int, user_id: int = None) -> 'ETLJob':
    """
    Get existing job or create new one (idempotent).
    
    Args:
        db: Database session
        task_id: Celery task ID
        input_file: Path to input file
        discipline_id: Discipline ID
        user_id: Optional user ID
        
    Returns:
        ETLJob instance
    """
    job = db.query(ETLJob).filter_by(task_id=task_id).first()
    
    if not job:
        job = ETLJob(
            task_id=task_id,
            input_file=os.path.basename(input_file),
            discipline_id=discipline_id,
            user_id=user_id,
            status=ETLJobStatus.PENDING
        )
        db.add(job)
        db.commit()
    
    return job


def _handle_task_failure(db, task_id: str, error_message: str):
    """
    Handle task failure by updating job record.
    
    Args:
        db: Database session
        task_id: Celery task ID
        error_message: Error description
    """
    try:
        job = db.query(ETLJob).filter_by(task_id=task_id).first()
        if job:
            job.status = ETLJobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = error_message[:2000]  # Limit error message length
            db.commit()
    except Exception:
        db.rollback()
