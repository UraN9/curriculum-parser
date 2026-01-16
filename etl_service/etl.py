"""
ETL Service for Academic Curriculum Parsing

Generates a structured Excel workbook from curriculum input data.
Responsibilities:
  - Extracts data from source Excel file with "План" sheet
  - Transforms and aggregates curriculum information by sections and themes
  - Loads formatted output to "Структура.xlsx" with proper styling
  
Key features:
  - Handles duplicate themes across different semesters
  - Calculates totals for hours by type (lectures, practical, lab work, etc.)
  - Applies professional formatting (bold, merged cells, centered alignment, color fills)
  - Automatically adjusts column widths
"""

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter


# ============================================================================
# Constants
# ============================================================================

BOLD_FONT = Font(bold=True)
CENTER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
SUMMARY_ROW_FILL = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

SECTION_MARKERS = ("РОЗДІЛ",)
THEME_MARKER = "Тема"
ACTIVITY_TYPES = ("Лекція", "Лабораторна", "Практична", "Самостійна")
SUMMARY_KEYWORDS = ("РОЗДІЛ", "РАЗОМ", "ВСЬОГО")

HOUR_COLUMN_TOTAL = 1
HOUR_COLUMN_LECTURES = 3
HOUR_COLUMN_PRACTICAL_LAB = 4
HOUR_COLUMN_SELF_WORK = 5


# ============================================================================
# Data Models
# ============================================================================

def _create_empty_theme() -> dict:
    """Create a dictionary representing an empty theme with hour counters."""
    return {
        "section": None,
        "theme": None,
        "total": 0,
        "lectures": 0,
        "practical": 0,
        "lab": 0,
        "individual": 0,
        "self": 0
    }


# ============================================================================
# ETL Processing Functions
# ============================================================================

def _extract_and_aggregate_data(input_file: str) -> tuple:
    """
    Extract curriculum data from Excel file and aggregate by sections and themes.
    
    Args:
        input_file: Path to input Excel file with "План" sheet
        
    Returns:
        Tuple of:
        - sections: List of unique section names in order
        - themes: Dict mapping (section, theme_name) to aggregated hour data
        - grand_totals: Dict with global hour statistics
    """
    # Load plan sheet without headers
    df_plan = pd.read_excel(input_file, sheet_name="План", header=None)
    
    # Initialize aggregation structures
    sections = []
    themes = {}
    grand_totals = {
        "total": 0,
        "lectures": 0,
        "practical": 0,
        "lab": 0,
        "individual": 0,
        "self": 0
    }
    
    current_section = None
    current_theme = None
    
    # Parse each row in the plan sheet
    for _, row in df_plan.iterrows():
        label = str(row[0]).strip() if pd.notnull(row[0]) else ""
        
        # Detect section header
        if label.startswith("РОЗДІЛ"):
            current_section = label
            sections.append(current_section)
            current_theme = None  # Reset theme when moving to new section
            continue
        
        # Detect theme header
        if label.startswith("Тема"):
            current_theme = label
            key = (current_section, current_theme)
            if key not in themes:
                theme_data = _create_empty_theme()
                theme_data["section"] = current_section
                theme_data["theme"] = current_theme
                themes[key] = theme_data
            continue
        
        # Process activity rows (Лекція, Лабораторна, etc.)
        if current_theme and any(label.startswith(activity) for activity in ACTIVITY_TYPES):
            # Extract hours from columns with safe defaults
            total_hours = row[HOUR_COLUMN_TOTAL] if pd.notnull(row[HOUR_COLUMN_TOTAL]) else 0
            lectures = row[HOUR_COLUMN_LECTURES] if pd.notnull(row[HOUR_COLUMN_LECTURES]) else 0
            prac_lab_hours = row[HOUR_COLUMN_PRACTICAL_LAB] if pd.notnull(row[HOUR_COLUMN_PRACTICAL_LAB]) else 0
            self_work_hours = row[HOUR_COLUMN_SELF_WORK] if pd.notnull(row[HOUR_COLUMN_SELF_WORK]) else 0
            
            key = (current_section, current_theme)
            theme_data = themes[key]
            
            # Aggregate hours by activity type
            if label.startswith("Лекція"):
                theme_data["lectures"] += lectures
            elif label.startswith(("Практична", "Семінарська")):
                theme_data["practical"] += prac_lab_hours
            elif label.startswith("Лабораторна"):
                theme_data["lab"] += prac_lab_hours
            elif label.startswith("Самостійна"):
                theme_data["self"] += self_work_hours
            
            # Calculate row total and aggregate
            row_total = total_hours or (lectures + prac_lab_hours + self_work_hours)
            theme_data["total"] += row_total
            
            # Update global totals
            grand_totals["total"] += row_total
            grand_totals["lectures"] += lectures
            grand_totals["practical"] += prac_lab_hours if label.startswith("Практична") else 0
            grand_totals["lab"] += prac_lab_hours if label.startswith("Лабораторна") else 0
            grand_totals["self"] += self_work_hours
    
    return sections, themes, grand_totals


def _build_structure_table(sections: list, themes: dict, grand_totals: dict) -> list:
    """
    Build the structure table with header rows, section content, and summary rows.
    
    Args:
        sections: List of section names
        themes: Dict of aggregated theme data
        grand_totals: Dict with global hour statistics
        
    Returns:
        List of lists representing the structured table
    """
    # Header rows (rows 1-4 in Excel)
    structure_data = [
        ["Назви змістових модулів і тем", "Кількість годин", "", "", "", "", ""],
        ["", "денна форма", "", "", "", "", ""],
        ["", "усього", "у тому числі", "", "", "", ""],
        ["", "", "лекції", "практичні, семінарські", "лабораторні", 
         "індивідуальні завдання", "самостійна робота"]
    ]
    
    # Content rows: sections and themes
    for section in sections:
        # Add section header row
        structure_data.append([section, "", "", "", "", "", ""])
        
        # Find all themes in this section
        section_themes = [t for t in themes.values() if t["section"] == section]
        
        # Add theme rows
        for theme in section_themes:
            structure_data.append([
                theme["theme"],
                theme["total"],
                theme["lectures"],
                theme["practical"],
                theme["lab"],
                theme["individual"],
                theme["self"]
            ])
        
        # Add section summary row
        total_hours = sum(t["total"] for t in section_themes)
        total_lectures = sum(t["lectures"] for t in section_themes)
        total_practical = sum(t["practical"] for t in section_themes)
        total_lab = sum(t["lab"] for t in section_themes)
        total_individual = sum(t["individual"] for t in section_themes)
        total_self = sum(t["self"] for t in section_themes)
        
        section_num = section.split()[1].rstrip('.')
        structure_data.append([
            f"Разом за розділом {section_num}",
            total_hours,
            total_lectures,
            total_practical,
            total_lab,
            total_individual,
            total_self
        ])
    
    # Add grand total row
    structure_data.append([
        "ВСЬОГО ПО НАВЧАЛЬНІЙ ДИСЦИПЛІНІ:",
        grand_totals["total"],
        grand_totals["lectures"],
        grand_totals["practical"],
        grand_totals["lab"],
        grand_totals["individual"],
        grand_totals["self"]
    ])
    
    return structure_data


def _write_data_to_worksheet(ws, structure_data: list) -> list:
    """
    Write raw data to worksheet and return row indices of section headers.
    
    Args:
        ws: Openpyxl worksheet object
        structure_data: List of lists representing table rows
        
    Returns:
        List of row indices (1-based) for section header rows
    """
    section_row_indices = []
    
    for row_idx, row_data in enumerate(structure_data, start=1):
        for col_idx, value in enumerate(row_data, start=1):
            ws.cell(row=row_idx, column=col_idx, value=value)
    
    # Identify section header rows (those with section names)
    for row_idx in range(5, ws.max_row + 1):  # Skip header rows 1-4
        cell_value = str(ws.cell(row=row_idx, column=1).value or "").strip()
        if cell_value.startswith("РОЗДІЛ"):
            section_row_indices.append(row_idx)
    
    return section_row_indices


def _apply_header_formatting(ws):
    """Apply formatting to header rows (rows 1-4)."""
    for row in ws.iter_rows(min_row=1, max_row=4):
        for cell in row:
            cell.font = BOLD_FONT
            cell.alignment = CENTER_ALIGNMENT


def _apply_content_formatting(ws):
    """Apply formatting to data rows (rows 5 and below)."""
    for row in ws.iter_rows(min_row=5, max_row=ws.max_row):
        first_cell = row[0]
        
        # Bold formatting for section/summary rows
        if first_cell.value:
            cell_text = str(first_cell.value).upper()
            if any(keyword in cell_text for keyword in SUMMARY_KEYWORDS):
                first_cell.font = BOLD_FONT
        
        # Center alignment for numeric columns (B-G)
        for cell in row[1:7]:
            if cell.value is not None:
                cell.alignment = CENTER_ALIGNMENT


def _apply_summary_row_styling(ws):
    """Apply background color to summary rows."""
    for row in ws.iter_rows(min_row=5, max_row=ws.max_row):
        first_cell = row[0]
        cell_text = str(first_cell.value or "").strip().upper()
        
        # Highlight summary rows with gray background
        if cell_text.startswith("РАЗОМ ЗА РОЗДІЛОМ") or cell_text.startswith("ВСЬОГО ПО НАВЧАЛЬНІЙ"):
            first_cell.font = BOLD_FONT
            for cell in row[:7]:  # Columns A-G
                cell.fill = SUMMARY_ROW_FILL


def _merge_header_cells(ws):
    """Merge header cells for proper table structure."""
    # First column merged for header label
    ws.merge_cells("A1:A4")
    
    # Hour category headers
    ws.merge_cells("B1:G1")  # "Кількість годин"
    ws.merge_cells("B2:G2")  # "денна форма"
    ws.merge_cells("C3:G3")  # "у тому числі"
    ws.merge_cells("B3:B4")  # "усього"


def _merge_section_cells(ws, section_row_indices: list):
    """Merge cells in section header rows across all columns."""
    for row_idx in section_row_indices:
        ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=7)
        
        # Apply formatting to merged cell
        cell = ws.cell(row=row_idx, column=1)
        cell.alignment = CENTER_ALIGNMENT
        cell.font = BOLD_FONT


def _auto_adjust_column_widths(ws):
    """Automatically adjust column widths based on content."""
    for col_idx in range(1, ws.max_column + 1):
        col_letter = get_column_letter(col_idx)
        max_content_length = 0
        
        # Find maximum content length in this column
        for row_idx in range(1, ws.max_row + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            try:
                if cell.value:
                    content_length = len(str(cell.value))
                    max_content_length = max(max_content_length, content_length)
            except Exception:
                # Skip merged cells and other problematic cells
                continue
        
        # Apply width with padding (more width for column A)
        width_padding = 4 if col_idx == 1 else 2.5
        ws.column_dimensions[col_letter].width = max_content_length + width_padding


from .validation import validate_plan_data, format_validation_report

# ============================================================================
# Main Function
# ============================================================================

def generate_structure(input_file: str, output_file: str = "Структура.xlsx") -> None:
    """
    Main ETL function: Extract → Transform → Load.
    
    Processes curriculum data from input Excel file and generates
    a properly formatted structure workbook.
    
    Args:
        input_file: Path to input Excel file containing "План" sheet
        output_file: Path where output Excel file will be saved
        
    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If "План" sheet not found in input file or validation fails
    """
    # Load the plan sheet
    df_plan = pd.read_excel(input_file, sheet_name="План", header=None)
    
    # === VALIDATE ===
    # Validate data integrity before processing
    validation_result = validate_plan_data(df_plan)
    
    # Print validation report
    print(format_validation_report(validation_result))
    
    # Stop if critical errors found
    if not validation_result.is_valid:
        raise ValueError(
            f"Validation failed with {validation_result.error_count} critical error(s). "
            "Please review and correct the input file."
        )
    
    # === EXTRACT ===
    sections, themes, grand_totals = _extract_and_aggregate_data(input_file)
    
    # === TRANSFORM ===
    structure_data = _build_structure_table(sections, themes, grand_totals)
    
    # === LOAD ===
    # Create workbook and worksheet
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Структура"
    
    # Write data and get section row positions
    section_row_indices = _write_data_to_worksheet(worksheet, structure_data)
    
    # Apply formatting
    _merge_header_cells(worksheet)
    _merge_section_cells(worksheet, section_row_indices)
    _apply_header_formatting(worksheet)
    _apply_content_formatting(worksheet)
    _apply_summary_row_styling(worksheet)
    _auto_adjust_column_widths(worksheet)
    
    # Save workbook
    workbook.save(output_file)
    print(f"\n✓ Generation completed successfully!")
    print(f"  Output file: {output_file}")
    print(f"  Sections: {len(sections)}")
    print(f"  Themes: {len(themes)}")
    print(f"  Total hours: {grand_totals['total']}")


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    generate_structure("НПр КН 2025.xlsx")
