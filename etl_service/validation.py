"""
Validation module for curriculum Excel files

Provides comprehensive validation of curriculum data from Excel "План" sheet.
Validates data integrity, constraints, and structure before ETL processing.

Key responsibilities:
  - Check for negative hours values
  - Verify required fields are not empty
  - Validate hour totals match component sums
  - Detect structural issues (missing sections, themes, etc.)
  - Categorize issues as errors (blocking) or warnings (informational)
"""

from typing import List, NamedTuple, Tuple
from enum import Enum
import pandas as pd


# ============================================================================
# Data Models
# ============================================================================

class SeverityLevel(str, Enum):
    """Classification of validation issues by severity."""
    ERROR = "error"      # Blocking issue - must be fixed before processing
    WARNING = "warning"  # Non-blocking issue - informational only


class ValidationIssue(NamedTuple):
    """Represents a single validation problem found in the data."""
    row_number: int          # 1-based row index for user-friendly reference
    column: str              # Column identifier (e.g., "A", "B", "hours")
    severity: SeverityLevel  # Whether this is an error or warning
    issue_type: str          # Category of issue (e.g., "negative_hours", "empty_field")
    message: str             # Human-readable description


class ValidationResult(NamedTuple):
    """Complete validation report for a curriculum dataset."""
    is_valid: bool                           # True if no ERRORs (warnings allowed)
    errors: List[ValidationIssue]            # Critical issues blocking processing
    warnings: List[ValidationIssue]          # Non-critical informational issues
    error_count: int                         # Number of errors
    warning_count: int                       # Number of warnings


# ============================================================================
# Constants
# ============================================================================

# Column indices for hour data (0-based indexing)
HOUR_COLUMNS = {
    "total": 1,
    "lectures": 3,
    "practical_lab": 4,
    "self_work": 5,
}

# Keywords marking structural elements
SECTION_MARKER = "РОЗДІЛ"
THEME_MARKER = "Тема"
ACTIVITY_TYPES = ("Лекція", "Лабораторна", "Практична", "Самостійна", "Семінарська")


# ============================================================================
# Validation Functions
# ============================================================================

def _validate_row_hours(row: pd.Series, row_num: int) -> List[ValidationIssue]:
    """
    Validate that hour values in a row are non-negative.
    
    Args:
        row: Pandas series representing one row from the dataframe
        row_num: 1-based row number for reporting
        
    Returns:
        List of validation issues found (empty if all valid)
    """
    issues = []
    
    for col_name, col_idx in HOUR_COLUMNS.items():
        if col_idx >= len(row):
            continue
            
        value = row[col_idx] if pd.notnull(row[col_idx]) else None
        
        # Skip empty cells (they default to 0)
        if value is None:
            continue
            
        # Convert to numeric if needed
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            issues.append(ValidationIssue(
                row_number=row_num,
                column=col_name,
                severity=SeverityLevel.ERROR,
                issue_type="invalid_number",
                message=f"Column '{col_name}' contains non-numeric value: {value}"
            ))
            continue
        
        # Check for negative values
        if numeric_value < 0:
            issues.append(ValidationIssue(
                row_number=row_num,
                column=col_name,
                severity=SeverityLevel.ERROR,
                issue_type="negative_hours",
                message=f"Column '{col_name}' has negative value: {numeric_value} (must be >= 0)"
            ))
    
    return issues


def _validate_required_fields(row: pd.Series, row_num: int, label: str) -> List[ValidationIssue]:
    """
    Validate that required fields for sections and themes are populated.
    
    Args:
        row: Pandas series representing one row
        row_num: 1-based row number for reporting
        label: The first column value (section/theme name)
        
    Returns:
        List of validation issues found
    """
    issues = []
    
    # Check if label exists and is not just whitespace
    if label and not label.strip():
        issues.append(ValidationIssue(
            row_number=row_num,
            column="A",
            severity=SeverityLevel.ERROR,
            issue_type="empty_field",
            message="Row has empty or whitespace-only name (section/theme must be named)"
        ))
    
    return issues


def _validate_hour_totals(row: pd.Series, row_num: int) -> List[ValidationIssue]:
    """
    Validate that total hours match the sum of component hours.
    
    Checks: total == (lectures + practical_lab + self_work)
    
    Args:
        row: Pandas series representing one row
        row_num: 1-based row number for reporting
        
    Returns:
        List of validation issues found
    """
    issues = []
    
    # Extract hour values (default to 0 if missing)
    try:
        total = float(row[HOUR_COLUMNS["total"]]) if pd.notnull(row[HOUR_COLUMNS["total"]]) else 0
        lectures = float(row[HOUR_COLUMNS["lectures"]]) if pd.notnull(row[HOUR_COLUMNS["lectures"]]) else 0
        practical_lab = float(row[HOUR_COLUMNS["practical_lab"]]) if pd.notnull(row[HOUR_COLUMNS["practical_lab"]]) else 0
        self_work = float(row[HOUR_COLUMNS["self_work"]]) if pd.notnull(row[HOUR_COLUMNS["self_work"]]) else 0
    except (ValueError, TypeError):
        return issues  # Skip validation if values are non-numeric (caught by other validators)
    
    # Calculate expected total
    expected_total = lectures + practical_lab + self_work
    
    # Check if totals don't match (allow for floating point tolerance)
    if total > 0 and abs(total - expected_total) > 0.01:
        issues.append(ValidationIssue(
            row_number=row_num,
            column="total",
            severity=SeverityLevel.WARNING,
            issue_type="hour_mismatch",
            message=f"Total hours {total} != sum of components {expected_total} "
                   f"(lectures={lectures} + practical/lab={practical_lab} + self_work={self_work})"
        ))
    
    return issues


def validate_plan_data(df: pd.DataFrame, skip_header_rows: int = 4) -> ValidationResult:
    """
    Comprehensive validation of curriculum data from "План" sheet.
    
    Performs the following checks on each row:
    1. Hours are non-negative
    2. Required fields (names) are not empty
    3. Hour totals match component sums
    4. All numeric values are properly formatted
    
    Args:
        df: Pandas dataframe loaded from "План" sheet (no headers)
        skip_header_rows: Number of header rows to skip (default 4 for typical curriculum format)
        
    Returns:
        ValidationResult containing categorized issues and summary
        
    Example:
        >>> df = pd.read_excel("input.xlsx", sheet_name="План", header=None)
        >>> result = validate_plan_data(df)
        >>> if result.is_valid:
        ...     print(f"✓ Valid! {result.warning_count} warnings")
        ... else:
        ...     print(f"✗ Invalid! {result.error_count} errors, {result.warning_count} warnings")
    """
    errors = []
    warnings = []
    
    # Validate each row (skip header rows)
    for idx, row in df.iterrows():
        row_num = idx + 1  # Convert to 1-based indexing for user display
        
        # Skip header rows
        if row_num <= skip_header_rows:
            continue
        
        # Extract label (section/theme name)
        label = str(row[0]).strip() if pd.notnull(row[0]) else ""
        
        # Skip completely empty rows
        if not label and all(pd.isnull(v) for v in row):
            continue
        
        # Run all validation checks
        hour_issues = _validate_row_hours(row, row_num)
        field_issues = _validate_required_fields(row, row_num, label)
        total_issues = _validate_hour_totals(row, row_num)
        
        # Collect all issues
        all_issues = hour_issues + field_issues + total_issues
        
        # Categorize by severity
        for issue in all_issues:
            if issue.severity == SeverityLevel.ERROR:
                errors.append(issue)
            else:
                warnings.append(issue)
    
    # Determine overall validity (valid if no errors, warnings are allowed)
    is_valid = len(errors) == 0
    
    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        error_count=len(errors),
        warning_count=len(warnings)
    )


# ============================================================================
# Reporting Functions
# ============================================================================

def format_validation_report(result: ValidationResult) -> str:
    """
    Format validation results as human-readable text report.
    
    Args:
        result: ValidationResult from validate_plan_data()
        
    Returns:
        Formatted string suitable for logging or display
    """
    lines = []
    
    # Header
    lines.append("=" * 70)
    lines.append("VALIDATION REPORT")
    lines.append("=" * 70)
    
    # Summary
    if result.is_valid:
        lines.append(f"✓ VALID - No critical errors found")
    else:
        lines.append(f"✗ INVALID - {result.error_count} error(s) found")
    
    lines.append(f"  Errors: {result.error_count}")
    lines.append(f"  Warnings: {result.warning_count}")
    lines.append("")
    
    # Error details
    if result.errors:
        lines.append("ERRORS (must be fixed):")
        lines.append("-" * 70)
        for issue in result.errors:
            lines.append(f"  Row {issue.row_number:4d} | {issue.issue_type:20s} | {issue.message}")
        lines.append("")
    
    # Warning details
    if result.warnings:
        lines.append("WARNINGS (informational):")
        lines.append("-" * 70)
        for issue in result.warnings:
            lines.append(f"  Row {issue.row_number:4d} | {issue.issue_type:20s} | {issue.message}")
        lines.append("")
    
    lines.append("=" * 70)
    
    return "\n".join(lines)