"""
Celery Configuration

Configures Celery with Redis as message broker.
"""

import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Create Celery application
celery_app = Celery(
    'curriculum_parser',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['celery_app.tasks']
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Kyiv',
    enable_utc=True,
    
    # Task execution settings
    task_acks_late=True,  # Acknowledge task after completion (better for reliability)
    task_reject_on_worker_lost=True,  # Reject task if worker dies
    
    # Result settings
    result_expires=3600,  # Results expire after 1 hour
    
    # Retry settings
    task_default_retry_delay=60,  # 1 minute delay between retries
    task_max_retries=3,  # Maximum 3 retries
    
    # Worker settings
    worker_prefetch_multiplier=1,  # Process one task at a time
    worker_concurrency=2,  # Number of concurrent workers
)
