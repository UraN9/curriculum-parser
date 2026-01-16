"""
ETL Error Logging Module

Provides comprehensive error logging for the ETL pipeline.
Logs errors to PostgreSQL database for persistent storage and analysis.

Key responsibilities:
  - Create unique ETL session identifiers
  - Log validation errors, database errors, and parse errors
  - Track error context (row numbers, field names, source data)
  - Generate error reports by session
  - Support error resolution workflow
"""

from typing import Optional, List, Any
from datetime import datetime
import uuid
import traceback

from sqlalchemy.orm import Session
from sqlalchemy import select, desc

# Import models (will be available when used in project)
# from app.models import ETLError, ErrorTypeEnum, SeverityEnum


# ============================================================================
# Constants
# ============================================================================

ERROR_TYPE_VALIDATION = "validation"
ERROR_TYPE_DATABASE = "database"
ERROR_TYPE_PARSE = "parse"
ERROR_TYPE_CONSTRAINT = "constraint"
ERROR_TYPE_UNKNOWN = "unknown"

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"


# ============================================================================
# Session Management
# ============================================================================

class ETLSession:
    """Represents a single ETL processing session."""
    
    def __init__(self, file_name: str = "unknown"):
        """
        Initialize a new ETL session.
        
        Args:
            file_name: Name of the file being processed
        """
        self.session_id = uuid.uuid4()
        self.file_name = file_name
        self.start_time = datetime.now()
        self.errors: List[dict] = []
    
    def __str__(self) -> str:
        return f"ETLSession(id={self.session_id}, file={self.file_name})"


# ============================================================================
# Error Logging Functions
# ============================================================================

def log_validation_error(
    db_session: Session,
    message: str,
    row_number: Optional[int] = None,
    field_name: Optional[str] = None,
    source_data: Optional[Any] = None,
    etl_session_id: Optional[uuid.UUID] = None,
    file_name: Optional[str] = None,
    severity: str = SEVERITY_ERROR
) -> None:
    """
    Log a validation error to the database.
    
    Validation errors occur during data validation phase (e.g., negative hours,
    empty required fields, sum mismatches).
    
    Args:
        db_session: SQLAlchemy database session
        message: Human-readable error description
        row_number: Excel row number where error occurred (optional)
        field_name: Name of the field with error (optional)
        source_data: The problematic data value (optional)
        etl_session_id: UUID of the ETL session (optional)
        file_name: Name of source file (optional)
        severity: "error" (blocking), "warning" (informational), or "info" (diagnostic)
    """
    _log_error_internal(
        db_session=db_session,
        error_type=ERROR_TYPE_VALIDATION,
        severity=severity,
        message=message,
        row_number=row_number,
        field_name=field_name,
        source_data=source_data,
        etl_session_id=etl_session_id,
        file_name=file_name
    )


def log_database_error(
    db_session: Session,
    message: str,
    row_number: Optional[int] = None,
    field_name: Optional[str] = None,
    source_data: Optional[Any] = None,
    etl_session_id: Optional[uuid.UUID] = None,
    file_name: Optional[str] = None,
    exception: Optional[Exception] = None
) -> None:
    """
    Log a database error to the database.
    
    Database errors occur during insert/update operations (e.g., constraint
    violations, connection errors, type mismatches).
    
    Args:
        db_session: SQLAlchemy database session
        message: Human-readable error description
        row_number: Excel row number associated with error (optional)
        field_name: Database field that caused error (optional)
        source_data: The data that triggered error (optional)
        etl_session_id: UUID of the ETL session (optional)
        file_name: Name of source file (optional)
        exception: Python exception object for stack trace (optional)
    """
    stack_trace = None
    if exception:
        stack_trace = traceback.format_exc()
    
    _log_error_internal(
        db_session=db_session,
        error_type=ERROR_TYPE_DATABASE,
        severity=SEVERITY_ERROR,
        message=message,
        row_number=row_number,
        field_name=field_name,
        source_data=source_data,
        etl_session_id=etl_session_id,
        file_name=file_name,
        stack_trace=stack_trace
    )


def log_parse_error(
    db_session: Session,
    message: str,
    row_number: Optional[int] = None,
    field_name: Optional[str] = None,
    source_data: Optional[Any] = None,
    etl_session_id: Optional[uuid.UUID] = None,
    file_name: Optional[str] = None,
    exception: Optional[Exception] = None
) -> None:
    """
    Log a parsing error to the database.
    
    Parse errors occur when data format is unexpected (e.g., invalid Excel
    structure, missing sheets, type conversion failures).
    
    Args:
        db_session: SQLAlchemy database session
        message: Human-readable error description
        row_number: Excel row number where error occurred (optional)
        field_name: Field that failed to parse (optional)
        source_data: The unparseable data (optional)
        etl_session_id: UUID of the ETL session (optional)
        file_name: Name of source file (optional)
        exception: Python exception object for stack trace (optional)
    """
    stack_trace = None
    if exception:
        stack_trace = traceback.format_exc()
    
    _log_error_internal(
        db_session=db_session,
        error_type=ERROR_TYPE_PARSE,
        severity=SEVERITY_ERROR,
        message=message,
        row_number=row_number,
        field_name=field_name,
        source_data=source_data,
        etl_session_id=etl_session_id,
        file_name=file_name,
        stack_trace=stack_trace
    )


def log_constraint_error(
    db_session: Session,
    message: str,
    field_name: Optional[str] = None,
    source_data: Optional[Any] = None,
    etl_session_id: Optional[uuid.UUID] = None,
    file_name: Optional[str] = None,
    exception: Optional[Exception] = None
) -> None:
    """
    Log a constraint violation error.
    
    Constraint errors occur when data violates database constraints
    (e.g., NOT NULL, UNIQUE, CHECK constraints).
    
    Args:
        db_session: SQLAlchemy database session
        message: Human-readable error description
        field_name: Constrained field name (optional)
        source_data: Data that violated constraint (optional)
        etl_session_id: UUID of the ETL session (optional)
        file_name: Name of source file (optional)
        exception: Python exception object for stack trace (optional)
    """
    stack_trace = None
    if exception:
        stack_trace = traceback.format_exc()
    
    _log_error_internal(
        db_session=db_session,
        error_type=ERROR_TYPE_CONSTRAINT,
        severity=SEVERITY_ERROR,
        message=message,
        field_name=field_name,
        source_data=source_data,
        etl_session_id=etl_session_id,
        file_name=file_name,
        stack_trace=stack_trace
    )


# ============================================================================
# Internal Implementation
# ============================================================================

def _log_error_internal(
    db_session: Session,
    error_type: str,
    severity: str,
    message: str,
    row_number: Optional[int] = None,
    field_name: Optional[str] = None,
    source_data: Optional[Any] = None,
    etl_session_id: Optional[uuid.UUID] = None,
    file_name: Optional[str] = None,
    stack_trace: Optional[str] = None
) -> None:
    """
    Internal function to log error to database.
    
    This is called by all public logging functions with proper error type
    and severity already determined.
    
    Args:
        db_session: SQLAlchemy database session
        error_type: Type of error (validation, database, parse, constraint, unknown)
        severity: Severity level (error, warning, info)
        message: Error message
        row_number: Row number if applicable
        field_name: Field name if applicable
        source_data: Source data if applicable
        etl_session_id: ETL session UUID if applicable
        file_name: File name if applicable
        stack_trace: Stack trace if applicable
    """
    try:
        # Import here to avoid circular imports
        from app.models import ETLError, ErrorTypeEnum, SeverityEnum
        
        # Create ETL error record
        error_record = ETLError(
            error_type=ErrorTypeEnum(error_type),
            severity=SeverityEnum(severity),
            message=message,
            row_number=row_number,
            field_name=field_name,
            source_data=str(source_data) if source_data is not None else None,
            etl_session_id=etl_session_id,
            file_name=file_name,
            stack_trace=stack_trace,
            resolved=False
        )
        
        # Add and commit
        db_session.add(error_record)
        db_session.commit()
        
    except Exception as e:
        # If logging fails, print to console as fallback
        print(f"⚠️  Failed to log error to database: {e}")
        print(f"   Original error: {message}")


# ============================================================================
# Error Reporting
# ============================================================================

def get_session_errors(
    db_session: Session,
    etl_session_id: uuid.UUID,
    severity_filter: Optional[str] = None
) -> List[dict]:
    """
    Retrieve all errors from a specific ETL session.
    
    Args:
        db_session: SQLAlchemy database session
        etl_session_id: UUID of the ETL session to retrieve
        severity_filter: Optional filter by severity ("error", "warning", "info")
        
    Returns:
        List of error dictionaries with all details
    """
    try:
        from app.models import ETLError
        
        query = select(ETLError).where(ETLError.etl_session_id == etl_session_id)
        
        if severity_filter:
            query = query.where(ETLError.severity == severity_filter)
        
        query = query.order_by(desc(ETLError.timestamp))
        
        errors = db_session.execute(query).scalars().all()
        
        return [
            {
                "id": e.id,
                "timestamp": e.timestamp,
                "error_type": e.error_type.value,
                "severity": e.severity.value,
                "row_number": e.row_number,
                "field_name": e.field_name,
                "message": e.message,
                "source_data": e.source_data,
                "file_name": e.file_name
            }
            for e in errors
        ]
    except Exception as e:
        print(f"⚠️  Failed to retrieve session errors: {e}")
        return []


def get_recent_errors(
    db_session: Session,
    limit: int = 50,
    severity_filter: Optional[str] = None
) -> List[dict]:
    """
    Retrieve recent errors from the database.
    
    Args:
        db_session: SQLAlchemy database session
        limit: Maximum number of errors to retrieve
        severity_filter: Optional filter by severity
        
    Returns:
        List of error dictionaries, ordered by most recent first
    """
    try:
        from app.models import ETLError
        
        query = select(ETLError).order_by(desc(ETLError.timestamp)).limit(limit)
        
        if severity_filter:
            query = select(ETLError).where(ETLError.severity == severity_filter).order_by(desc(ETLError.timestamp)).limit(limit)
        
        errors = db_session.execute(query).scalars().all()
        
        return [
            {
                "id": e.id,
                "timestamp": e.timestamp,
                "error_type": e.error_type.value,
                "severity": e.severity.value,
                "row_number": e.row_number,
                "field_name": e.field_name,
                "message": e.message,
                "source_data": e.source_data,
                "session_id": str(e.etl_session_id) if e.etl_session_id else None,
                "file_name": e.file_name
            }
            for e in errors
        ]
    except Exception as e:
        print(f"⚠️  Failed to retrieve recent errors: {e}")
        return []


def format_error_report(
    errors: List[dict],
    session_id: Optional[str] = None
) -> str:
    """
    Format a list of errors as a human-readable report.
    
    Args:
        errors: List of error dictionaries from get_session_errors() or get_recent_errors()
        session_id: Optional session ID to include in report header
        
    Returns:
        Formatted error report string
    """
    lines = []
    
    # Header
    lines.append("=" * 80)
    lines.append("ETL ERROR REPORT")
    if session_id:
        lines.append(f"Session ID: {session_id}")
    lines.append("=" * 80)
    
    if not errors:
        lines.append("✓ No errors found")
        lines.append("=" * 80)
        return "\n".join(lines)
    
    # Summary
    error_count = sum(1 for e in errors if e["severity"] == "error")
    warning_count = sum(1 for e in errors if e["severity"] == "warning")
    info_count = sum(1 for e in errors if e["severity"] == "info")
    
    lines.append(f"\nSummary:")
    lines.append(f"  Errors:   {error_count}")
    lines.append(f"  Warnings: {warning_count}")
    lines.append(f"  Info:     {info_count}")
    lines.append("")
    
    # Group by severity
    errors_by_severity = {
        "error": [e for e in errors if e["severity"] == "error"],
        "warning": [e for e in errors if e["severity"] == "warning"],
        "info": [e for e in errors if e["severity"] == "info"]
    }
    
    for severity in ["error", "warning", "info"]:
        if errors_by_severity[severity]:
            lines.append(f"\n{severity.upper()}S:")
            lines.append("-" * 80)
            
            for error in errors_by_severity[severity]:
                # Basic info
                timestamp = error["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if error["timestamp"] else "N/A"
                lines.append(f"  [{timestamp}] {error['error_type']} - {error['message']}")
                
                # Additional context if available
                if error.get("row_number"):
                    lines.append(f"    Row: {error['row_number']}")
                if error.get("field_name"):
                    lines.append(f"    Field: {error['field_name']}")
                if error.get("source_data"):
                    lines.append(f"    Data: {error['source_data'][:100]}")
                if error.get("file_name"):
                    lines.append(f"    File: {error['file_name']}")
                
                lines.append("")
    
    lines.append("=" * 80)
    return "\n".join(lines)
