"""
ETL Service Package

Provides extract, transform, load (ETL) functionality for curriculum Excel files.
Includes validation, error logging, and structured output generation.
"""

from .etl import generate_structure
from .validation import validate_plan_data, format_validation_report, ValidationResult
from .etl_logger import (
    ETLSession,
    log_validation_error,
    log_database_error,
    log_parse_error,
    log_constraint_error,
    get_session_errors,
    get_recent_errors,
    format_error_report
)
from .db_loader import (
    load_activity_types,
    load_control_forms,
    find_or_create_semester,
    save_section,
    save_theme,
    save_activity,
    extract_semester_number,
    extract_activity_type,
    extract_control_form,
    commit_changes
)

__all__ = [
    "generate_structure",
    "validate_plan_data",
    "format_validation_report",
    "ValidationResult",
    "ETLSession",
    "log_validation_error",
    "log_database_error",
    "log_parse_error",
    "log_constraint_error",
    "get_session_errors",
    "get_recent_errors",
    "format_error_report",
    "load_activity_types",
    "load_control_forms",
    "find_or_create_semester",
    "save_section",
    "save_theme",
    "save_activity",
    "extract_semester_number",
    "extract_activity_type",
    "extract_control_form",
    "commit_changes",
]
