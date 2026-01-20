"""
Database Loader for Curriculum ETL

Handles loading parsed curriculum data into PostgreSQL with proper:
  - UPSERT logic (INSERT OR UPDATE on duplicate key)
  - Foreign key relationship management
  - Reference data initialization (activity types, control forms, semesters)
  - Data extraction and transformation from Excel labels

Module functions:
  - load_activity_types(db_session)
  - load_control_forms(db_session)
  - find_or_create_semester(db_session, number)
  - save_section(db_session, name, discipline_id, semester_id)
  - save_theme(db_session, name, section_id, total_hours)
  - save_activity(db_session, name, type_id, hours, theme_id, control_form_id)
  - extract_semester_number(text)
  - extract_activity_type(label)
  - extract_control_form(value)
"""

import re
import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models import ActivityType, ControlForm, Semester, Section, Theme, Activity
from .etl_logger import log_database_error


# ============================================================================
# Reference Data Loaders (One-time initialization)
# ============================================================================

def load_activity_types(db_session) -> None:
    """
    Load standard activity types into database.
    
    Creates reference data for curriculum activities:
    - 1: Лекція (Lecture)
    - 2: Практична (Practical/Seminar)
    - 3: Лабораторна (Laboratory)
    - 4: Самостійна (Self-study)
    
    Uses UPSERT logic: if type exists, skip; if not, insert.
    
    Args:
        db_session: SQLAlchemy session
        
    Returns:
        None
        
    Raises:
        Exception: Database integrity errors (logged and re-raised)
    """
    activity_types_data = [
        {"id": 1, "name": "Лекція"},
        {"id": 2, "name": "Практична"},
        {"id": 3, "name": "Лабораторна"},
        {"id": 4, "name": "Самостійна"}
    ]
    
    try:
        for type_data in activity_types_data:
            # Check if already exists
            existing = db_session.query(ActivityType).filter_by(
                id=type_data["id"]
            ).first()
            
            if not existing:
                new_type = ActivityType(
                    id=type_data["id"],
                    name=type_data["name"]
                )
                db_session.add(new_type)
        
        db_session.commit()
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to load activity types: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error loading activity types: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


def load_control_forms(db_session) -> None:
    """
    Load standard control form types into database.
    
    Creates reference data for form of control/assessment:
    - 1: опитування (Quiz/Oral exam)
    - 2: захист (Project defense/Practical exam)
    - 3: конспект (Lecture notes submission)
    
    Uses UPSERT logic: if form exists, skip; if not, insert.
    
    Args:
        db_session: SQLAlchemy session
        
    Returns:
        None
        
    Raises:
        Exception: Database integrity errors (logged and re-raised)
    """
    control_forms_data = [
        {"id": 1, "name": "опитування"},
        {"id": 2, "name": "захист"},
        {"id": 3, "name": "конспект"}
    ]
    
    try:
        for form_data in control_forms_data:
            # Check if already exists
            existing = db_session.query(ControlForm).filter_by(
                id=form_data["id"]
            ).first()
            
            if not existing:
                new_form = ControlForm(
                    id=form_data["id"],
                    name=form_data["name"]
                )
                db_session.add(new_form)
        
        db_session.commit()
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to load control forms: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error loading control forms: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Semester Management
# ============================================================================

def find_or_create_semester(db_session, number: int, weeks: int = 17, 
                           hours_per_week: int = 10) -> int:
    """
    Find existing semester or create new one if not exists.
    
    Implements UPSERT pattern: if semester with given number exists,
    return its ID; otherwise create new semester with provided parameters.
    
    Args:
        db_session: SQLAlchemy session
        number: Semester number (e.g., 5 for "5 СЕМЕСТР")
        weeks: Number of weeks in semester (default: 17)
        hours_per_week: Hours per week (default: 10)
        
    Returns:
        int: Semester ID (existing or newly created)
        
    Raises:
        Exception: Database errors (logged and re-raised)
    """
    try:
        # Check if semester exists
        existing_semester = db_session.query(Semester).filter_by(
            number=number
        ).first()
        
        if existing_semester:
            return existing_semester.id
        
        # Create new semester
        new_semester = Semester(
            number=number,
            weeks=weeks,
            hours_per_week=hours_per_week
        )
        db_session.add(new_semester)
        db_session.flush()  # Get the ID without committing
        
        return new_semester.id
        
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to create semester {number}: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error with semester {number}: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Section Management (UPSERT)
# ============================================================================

def save_section(db_session, name: str, discipline_id: int, 
                semester_id: int) -> int:
    """
    Save section (РОЗДІЛ) to database with UPSERT logic.
    
    If section with same name, discipline_id, and semester_id exists,
    return its ID. Otherwise create new section and return ID.
    
    Args:
        db_session: SQLAlchemy session
        name: Section name (e.g., "РОЗДІЛ 1. ПОНЯТТЯ БАЗ ДАНИХ ТА СУБД")
        discipline_id: Foreign key to disciplines table
        semester_id: Foreign key to semesters table
        
    Returns:
        int: Section ID (existing or newly created)
        
    Raises:
        Exception: Database errors (logged and re-raised)
    """
    try:
        # Check if section exists
        existing_section = db_session.query(Section).filter_by(
            name=name,
            discipline_id=discipline_id,
            semester_id=semester_id
        ).first()
        
        if existing_section:
            return existing_section.id
        
        # Create new section
        new_section = Section(
            name=name,
            discipline_id=discipline_id,
            semester_id=semester_id
        )
        db_session.add(new_section)
        db_session.flush()  # Get the ID without committing
        
        return new_section.id
        
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to save section '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error saving section '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Theme Management (UPSERT)
# ============================================================================

def save_theme(db_session, name: str, section_id: int, 
              total_hours: int) -> int:
    """
    Save theme (Тема) to database with UPSERT logic.
    
    If theme with same name and section_id exists, update total_hours
    and return its ID. Otherwise create new theme and return ID.
    
    Args:
        db_session: SQLAlchemy session
        name: Theme name (e.g., "Тема 1.1 Основні поняття баз даних")
        section_id: Foreign key to sections table
        total_hours: Total hours for theme
        
    Returns:
        int: Theme ID (existing or newly created)
        
    Raises:
        Exception: Database errors (logged and re-raised)
    """
    try:
        # Check if theme exists
        existing_theme = db_session.query(Theme).filter_by(
            name=name,
            section_id=section_id
        ).first()
        
        if existing_theme:
            # Update total_hours in case of duplicate load
            existing_theme.total_hours = total_hours
            db_session.flush()
            return existing_theme.id
        
        # Create new theme
        new_theme = Theme(
            name=name,
            section_id=section_id,
            total_hours=total_hours
        )
        db_session.add(new_theme)
        db_session.flush()  # Get the ID without committing
        
        return new_theme.id
        
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to save theme '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error saving theme '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Activity Management (UPSERT)
# ============================================================================

def save_activity(db_session, name: str, type_id: int, hours: int,
                 theme_id: int, control_form_id: int = None) -> int:
    """
    Save activity to database with UPSERT logic.
    
    If activity with same name and theme_id exists, update type_id, hours,
    and control_form_id. Otherwise create new activity and return ID.
    
    Args:
        db_session: SQLAlchemy session
        name: Activity name (e.g., "Лекція 1. Основи баз даних")
        type_id: Foreign key to activity_types (1=Лекція, 2=Практична, etc.)
        hours: Hours for activity
        theme_id: Foreign key to themes table
        control_form_id: Optional foreign key to control_forms table
        
    Returns:
        int: Activity ID (existing or newly created)
        
    Raises:
        Exception: Database errors (logged and re-raised)
    """
    try:
        # Check if activity exists
        existing_activity = db_session.query(Activity).filter_by(
            name=name,
            theme_id=theme_id
        ).first()
        
        if existing_activity:
            # Update fields in case of duplicate load
            existing_activity.type_id = type_id
            existing_activity.hours = hours
            existing_activity.control_form_id = control_form_id
            db_session.flush()
            return existing_activity.id
        
        # Create new activity
        new_activity = Activity(
            name=name,
            type_id=type_id,
            hours=hours,
            theme_id=theme_id,
            control_form_id=control_form_id
        )
        db_session.add(new_activity)
        db_session.flush()  # Get the ID without committing
        
        return new_activity.id
        
    except IntegrityError as e:
        db_session.rollback()
        error_msg = f"Failed to save activity '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error saving activity '{name}': {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Data Extraction Helpers
# ============================================================================

def extract_semester_number(text: str) -> int:
    """
    Extract semester number from text.
    
    Parses strings like "5 СЕМЕСТР" and extracts the numeric part.
    
    Args:
        text: Text potentially containing semester number (e.g., "5 СЕМЕСТР")
        
    Returns:
        int: Extracted semester number, or None if not found
        
    Examples:
        >>> extract_semester_number("5 СЕМЕСТР")
        5
        >>> extract_semester_number("СЕМЕСТР 3")
        3
        >>> extract_semester_number("invalid")
        None
    """
    if not text:
        return None
    
    match = re.search(r'\d+', str(text).strip())
    return int(match.group()) if match else None


def extract_activity_type(label: str) -> int:
    """
    Extract activity type ID from activity label.
    
    Maps activity type prefixes to IDs:
    - "Лекція" → 1
    - "Практична" or "Семінарська" → 2
    - "Лабораторна" → 3
    - "Самостійна" → 4
    
    Args:
        label: Activity label (e.g., "Лекція 1. Основи баз даних")
        
    Returns:
        int: Activity type ID (1-4), or None if not recognized
        
    Examples:
        >>> extract_activity_type("Лекція 1. Основи баз даних")
        1
        >>> extract_activity_type("Практична робота №1")
        2
        >>> extract_activity_type("Лабораторна робота №2")
        3
        >>> extract_activity_type("Самостійна робота №1")
        4
        >>> extract_activity_type("Unknown type")
        None
    """
    if not label:
        return None
    
    label = str(label).strip()
    
    if label.startswith("Лекція"):
        return 1
    elif label.startswith(("Практична", "Семінарська", "Практичні")):
        return 2
    elif label.startswith("Лабораторна"):
        return 3
    elif label.startswith("Самостійна"):
        return 4
    else:
        return None  # Unknown activity type


def extract_control_form(value) -> int:
    """
    Extract control form ID from form name.
    
    Maps control form names to IDs with fuzzy matching:
    - Contains "опит" (опитування) → 1
    - Contains "захист" (захист) → 2
    - Contains "конспект" or "консп" → 3
    - NaN, None, empty string → None
    
    Uses case-insensitive matching and strips whitespace.
    
    Args:
        value: Control form name/value (str, pd.NA, None, etc.)
        
    Returns:
        int: Control form ID (1-3), or None if not recognized or empty
        
    Examples:
        >>> extract_control_form("опитування")
        1
        >>> extract_control_form("Захист проекту")
        2
        >>> extract_control_form("конспект")
        3
        >>> extract_control_form("КОНСПЕКТУВАННЯ")
        3
        >>> extract_control_form(np.nan)
        None
        >>> extract_control_form("")
        None
        >>> extract_control_form(None)
        None
    """
    # Handle null/missing values
    if pd.isna(value) or value is None or value == '':
        return None
    
    # Convert to lowercase string and strip whitespace
    value_str = str(value).lower().strip()
    
    # Empty after conversion
    if not value_str:
        return None
    
    # Fuzzy matching
    if "опит" in value_str:  # Handles "опитування", "опиту", etc.
        return 1
    elif "захист" in value_str:  # Handles "захист", "Захист проекту", etc.
        return 2
    elif "конспект" in value_str or "консп" in value_str:  # Handles various spellings
        return 3
    else:
        return None  # Unknown control form


# ============================================================================
# Batch Operations Helper
# ============================================================================

def commit_changes(db_session) -> None:
    """
    Commit pending database changes.
    
    Safely commits all pending changes accumulated via flush() calls.
    Provides centralized error handling for commit operations.
    
    Args:
        db_session: SQLAlchemy session
        
    Raises:
        Exception: Database commit errors (logged and re-raised)
    """
    try:
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        error_msg = f"Failed to commit database changes: {str(e)}"
        log_database_error(db_session, error_msg, e)
        raise


# ============================================================================
# Summary Views Refresh
# ============================================================================

def refresh_summaries(db_session) -> dict:
    """
    Refresh all materialized summary views.
    
    Calls the PostgreSQL function refresh_all_summaries() which refreshes:
    - mv_section_summary
    - mv_theme_summary
    - mv_activity_type_summary
    - mv_semester_summary
    - mv_control_form_summary
    
    Args:
        db_session: SQLAlchemy session
        
    Returns:
        dict with refresh status and any errors
        
    Note:
        Views must be created first by running db/summary_views.sql
    """
    from sqlalchemy import text
    
    result = {
        "success": False,
        "views_refreshed": 0,
        "error": None
    }
    
    views = [
        "mv_section_summary",
        "mv_theme_summary", 
        "mv_activity_type_summary",
        "mv_semester_summary",
        "mv_control_form_summary"
    ]
    
    try:
        # Try to call the PostgreSQL function first
        try:
            db_session.execute(text("SELECT refresh_all_summaries()"))
            db_session.commit()
            result["success"] = True
            result["views_refreshed"] = len(views)
            return result
        except Exception:
            # Function doesn't exist, try refreshing views individually
            db_session.rollback()
        
        # Fallback: refresh each view individually
        refreshed = 0
        for view_name in views:
            try:
                db_session.execute(text(f"REFRESH MATERIALIZED VIEW {view_name}"))
                refreshed += 1
            except Exception as e:
                # View might not exist yet
                db_session.rollback()
                continue
        
        if refreshed > 0:
            db_session.commit()
            result["success"] = True
            result["views_refreshed"] = refreshed
        else:
            result["error"] = "No views found. Run db/summary_views.sql first."
            
    except Exception as e:
        db_session.rollback()
        result["error"] = str(e)
        
    return result
