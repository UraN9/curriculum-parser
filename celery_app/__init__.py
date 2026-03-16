"""
Celery Application for Asynchronous ETL Processing

This module initializes and configures the Celery application
for running ETL tasks asynchronously.
"""

from .celery_config import celery_app

__all__ = ['celery_app']
