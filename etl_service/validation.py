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

# Column indices for hour data (0-based indexing) — used as positional fallback
HOUR_COLUMNS = {
    "total": 1,
    "lectures": 3,
    "practical_lab": 4,
    "self_work": 5,
}

# Ukrainian keywords for auto-detecting column positions from Excel header rows
_DETECT_KEYWORDS = {
    "total":         ["усього", "загальна кількість", "всього год"],
    "lectures":      ["лекції", "лекц."],
    "practical_lab": ["практичн", "семінарськ"],
    "lab":           ["лаборатор"],
    "self_work":     ["самостійна"],
    "control_form":  ["форма контр", "вид контр", "контрол"],
}

# Activity label prefixes mapped to their hour field
_ACTIVITY_TO_FIELD = [
    ("Лекція",      "lectures"),
    ("Практична",   "practical_lab"),
    ("Семінарська", "practical_lab"),
    ("Лабораторна", "practical_lab"),
    ("Самостійна",  "self_work"),
]


def detect_column_map(df: pd.DataFrame, search_rows: int = 10) -> dict:
    """
    Auto-detect column positions using a two-pass strategy.

    Pass 1 — header keyword scan: looks for Ukrainian column header words in the
    first search_rows rows of df.

    Pass 2 — data-driven scan: for any required column still missing after pass 1,
    infers positions from actual data rows:
      - "total"        → first positive numeric column in "Тема" rows.
      - "lectures"     → positive numeric columns in "Лекція" rows (excluding total).
      - "practical_lab"→ positive numeric columns in "Практична/Семінарська/Лабораторна" rows.
      - "self_work"    → positive numeric columns in "Самостійна" rows.

    Falls back to HOUR_COLUMNS positional defaults for fields still unresolved after
    both passes.

    Required result keys: total, lectures, practical_lab, self_work.
    Optional result keys: lab (separate from practical_lab), control_form.
    """
    from collections import Counter

    found: dict = {}

    # Prefixes that identify data rows (not header rows); skip these in Pass 1
    _DATA_ROW_PREFIXES = (
        "лекція", "практична", "семінарська", "лабораторна",
        "самостійна", "тема", "розділ", "семестр",
    )

    # --- Pass 1: header keyword scan ---
    for row_idx in range(min(search_rows, len(df))):
        # Skip rows that are clearly data rows (activity/section/theme labels)
        row_label = str(df.iloc[row_idx, 0]).lower().strip() if pd.notnull(df.iloc[row_idx, 0]) else ""
        if any(row_label.startswith(p) for p in _DATA_ROW_PREFIXES):
            continue

        for col_idx, val in enumerate(df.iloc[row_idx]):
            if col_idx == 0:  # col 0 is always the name column, never a header for hours
                continue
            if pd.isnull(val):
                continue
            cell = str(val).lower().strip()

            if "total" not in found and any(k in cell for k in _DETECT_KEYWORDS["total"]):
                found["total"] = col_idx
            if "lectures" not in found and any(k in cell for k in _DETECT_KEYWORDS["lectures"]):
                found["lectures"] = col_idx
            if "practical_lab" not in found and any(k in cell for k in _DETECT_KEYWORDS["practical_lab"]):
                found["practical_lab"] = col_idx
            if "lab" not in found and any(k in cell for k in _DETECT_KEYWORDS["lab"]):
                found["lab"] = col_idx
            if "self_work" not in found and any(k in cell for k in _DETECT_KEYWORDS["self_work"]):
                found["self_work"] = col_idx
            if "control_form" not in found and any(k in cell for k in _DETECT_KEYWORDS["control_form"]):
                found["control_form"] = col_idx

    if "lab" in found and "practical_lab" not in found:
        found["practical_lab"] = found["lab"]

    # --- Pass 2: data-driven scan for still-missing required columns ---
    required = {"total", "lectures", "practical_lab", "self_work"}
    missing = required - found.keys()

    if missing:
        type_cols: dict = {f: [] for f in missing}

        # Sub-pass 2a: detect total from "Тема" rows (first positive numeric col)
        if "total" in missing:
            for _, row in df.iterrows():
                label = str(row[0]).strip() if pd.notnull(row[0]) else ""
                if label.startswith("Тема"):
                    for ci in range(1, len(row)):
                        v = row[ci]
                        if pd.notnull(v) and isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0:
                            type_cols["total"].append(ci)
                            break
            if type_cols["total"]:
                found["total"] = Counter(type_cols["total"]).most_common(1)[0][0]
            else:
                # No Тема rows with data — use positional default
                found["total"] = HOUR_COLUMNS["total"]

        # Sub-pass 2b: detect type-specific cols from activity rows, excluding total
        known_total = found.get("total")
        missing = required - found.keys()   # recompute after total resolved

        for _, row in df.iterrows():
            label = str(row[0]).strip() if pd.notnull(row[0]) else ""
            for prefix, field in _ACTIVITY_TO_FIELD:
                if field in missing and label.startswith(prefix):
                    for ci in range(1, len(row)):
                        v = row[ci]
                        if (pd.notnull(v) and isinstance(v, (int, float))
                                and not isinstance(v, bool) and v > 0
                                and ci != known_total):
                            type_cols[field].append(ci)
                    break

        for field, cols in type_cols.items():
            if cols and field not in found:
                found[field] = Counter(cols).most_common(1)[0][0]

    # --- Final: fill any still-missing fields with positional defaults ---
    missing = required - found.keys()
    if missing:
        print(f"  ⚠ Column detection incomplete (not found: {missing}), using positional defaults")
        for field in missing:
            found[field] = HOUR_COLUMNS[field]

    detected = {k: v for k, v in found.items() if k in required}
    print(f"  ✓ Column map: {detected}")
    return found

# Keywords marking structural elements
SECTION_MARKER = "РОЗДІЛ"
THEME_MARKER = "Тема"
ACTIVITY_TYPES = ("Лекція", "Лабораторна", "Практична", "Самостійна", "Семінарська")


# ============================================================================
# Validation Functions
# ============================================================================

def _validate_row_hours(row: pd.Series, row_num: int, col_map: dict = None) -> List[ValidationIssue]:
    """
    Validate that hour values in a row are non-negative.

    Args:
        row: Pandas series representing one row from the dataframe
        row_num: 1-based row number for reporting
        col_map: Column index mapping (uses HOUR_COLUMNS defaults if None)

    Returns:
        List of validation issues found (empty if all valid)
    """
    issues = []
    active_cols = {k: v for k, v in (col_map or HOUR_COLUMNS).items()
                   if k in ("total", "lectures", "practical_lab", "self_work")}

    for col_name, col_idx in active_cols.items():
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


def _validate_hour_totals(row: pd.Series, row_num: int, col_map: dict = None) -> List[ValidationIssue]:
    """
    Validate that total hours match the sum of component hours.

    Checks: total == (lectures + practical_lab + self_work)

    Args:
        row: Pandas series representing one row
        row_num: 1-based row number for reporting
        col_map: Column index mapping (uses HOUR_COLUMNS defaults if None)

    Returns:
        List of validation issues found
    """
    issues = []
    cm = col_map or HOUR_COLUMNS

    # Extract hour values (default to 0 if missing)
    try:
        total = float(row[cm["total"]]) if pd.notnull(row[cm["total"]]) else 0
        lectures = float(row[cm["lectures"]]) if pd.notnull(row[cm["lectures"]]) else 0
        practical_lab = float(row[cm["practical_lab"]]) if pd.notnull(row[cm["practical_lab"]]) else 0
        self_work = float(row[cm["self_work"]]) if pd.notnull(row[cm["self_work"]]) else 0
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

    # Auto-detect column positions from the header rows
    col_map = detect_column_map(df)

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
        hour_issues = _validate_row_hours(row, row_num, col_map)
        field_issues = _validate_required_fields(row, row_num, label)
        total_issues = _validate_hour_totals(row, row_num, col_map)
        
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