"""
Custom validators using Regular Expressions.

Best practices:
- https://www.geeksforgeeks.org/dsa/write-regular-expressions/
- https://regex101.com/
"""
import re
from marshmallow import ValidationError


# ============================================================
# REGEX PATTERNS
# ============================================================

# Ukrainian text: letters, digits, spaces, punctuation
# Allows: А-Я, а-я, І, Ї, Є, Ґ, digits, spaces, common punctuation
UKRAINIAN_TEXT_PATTERN = re.compile(
    r'^[А-ЩЬЮЯҐЄІЇа-щьюяґєії\w\s\-\.\,\:\;\'\"\(\)\№\d]+$',
    re.UNICODE
)

# Name pattern: at least 2 characters, letters/digits/spaces/punctuation
NAME_PATTERN = re.compile(
    r'^[\w\s\-\.\,\:\;\'\"\(\)\№А-ЩЬЮЯҐЄІЇа-щьюяґєії]{2,200}$',
    re.UNICODE
)

# Email pattern: standard email format
EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)

# Full name: Ukrainian letters, spaces, hyphens, apostrophes
FULL_NAME_PATTERN = re.compile(
    r'^[А-ЩЬЮЯҐЄІЇа-щьюяґєії\s\-\']+$',
    re.UNICODE
)


# ============================================================
# VALIDATOR FUNCTIONS
# ============================================================

def validate_ukrainian_text(value):
    """
    Validate that text contains valid Ukrainian characters.
    
    Allows: Ukrainian letters, digits, spaces, common punctuation.
    
    Args:
        value: String to validate
        
    Raises:
        ValidationError: If text contains invalid characters
    """
    if not value or not value.strip():
        raise ValidationError("Text cannot be empty.")
    
    if not UKRAINIAN_TEXT_PATTERN.match(value):
        raise ValidationError(
            "Text must contain only valid characters "
            "(Ukrainian letters, digits, spaces, punctuation)."
        )


def validate_name(value):
    """
    Validate entity name (discipline, section, theme).
    
    Requirements:
    - 2-200 characters
    - Letters, digits, spaces, punctuation allowed
    
    Args:
        value: Name string to validate
        
    Raises:
        ValidationError: If name is invalid
    """
    if not value or not value.strip():
        raise ValidationError("Name cannot be empty.")
    
    value = value.strip()
    
    if len(value) < 2:
        raise ValidationError("Name must be at least 2 characters long.")
    
    if len(value) > 200:
        raise ValidationError("Name must not exceed 200 characters.")
    
    if not NAME_PATTERN.match(value):
        raise ValidationError(
            "Name contains invalid characters. "
            "Use letters, digits, spaces, and common punctuation."
        )


def validate_email(value):
    """
    Validate email address format using regex.
    
    Pattern: username@domain.tld
    
    Args:
        value: Email string to validate
        
    Raises:
        ValidationError: If email format is invalid
    """
    if not value or not value.strip():
        raise ValidationError("Email cannot be empty.")
    
    if not EMAIL_PATTERN.match(value):
        raise ValidationError(
            "Invalid email format. Expected: example@domain.com"
        )


def validate_full_name(value):
    """
    Validate person's full name (Ukrainian).
    
    Requirements:
    - Only Ukrainian letters, spaces, hyphens, apostrophes
    - 2-100 characters
    
    Args:
        value: Full name string to validate
        
    Raises:
        ValidationError: If name is invalid
    """
    if not value or not value.strip():
        raise ValidationError("Full name cannot be empty.")
    
    value = value.strip()
    
    if len(value) < 2:
        raise ValidationError("Full name must be at least 2 characters long.")
    
    if len(value) > 100:
        raise ValidationError("Full name must not exceed 100 characters.")
    
    if not FULL_NAME_PATTERN.match(value):
        raise ValidationError(
            "Full name must contain only Ukrainian letters, "
            "spaces, hyphens, and apostrophes."
        )


def validate_course(value):
    """
    Validate course number.
    
    Requirements:
    - Integer between 1 and 6
    
    Args:
        value: Course number to validate
        
    Raises:
        ValidationError: If course is invalid
    """
    if not isinstance(value, int):
        raise ValidationError("Course must be an integer.")
    
    if value < 1 or value > 6:
        raise ValidationError("Course must be between 1 and 6.")


def validate_ects_credits(value):
    """
    Validate ECTS credits.
    
    Requirements:
    - Number between 0.5 and 30.0
    - Step of 0.5
    
    Args:
        value: ECTS credits value to validate
        
    Raises:
        ValidationError: If credits are invalid
    """
    try:
        credits = float(value)
    except (TypeError, ValueError):
        raise ValidationError("ECTS credits must be a number.")
    
    if credits < 0.5 or credits > 30.0:
        raise ValidationError("ECTS credits must be between 0.5 and 30.0.")


def validate_hours(value):
    """
    Validate hours value.
    
    Requirements:
    - Positive integer
    - Maximum 100 hours per activity
    
    Args:
        value: Hours value to validate
        
    Raises:
        ValidationError: If hours are invalid
    """
    if not isinstance(value, int):
        raise ValidationError("Hours must be an integer.")
    
    if value < 1:
        raise ValidationError("Hours must be at least 1.")
    
    if value > 100:
        raise ValidationError("Hours cannot exceed 100 per activity.")


def validate_positive_integer(value, field_name="Value"):
    """
    Validate that value is a positive integer.
    
    Args:
        value: Value to validate
        field_name: Name for error messages
        
    Raises:
        ValidationError: If value is not a positive integer
    """
    if not isinstance(value, int):
        raise ValidationError(f"{field_name} must be an integer.")
    
    if value < 1:
        raise ValidationError(f"{field_name} must be a positive integer.")
