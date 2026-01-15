"""
ETL Service: Generate "Структура.xlsx" from any input file with "План" sheet
Correctly aggregates duplicate themes (e.g. same theme in different semesters),
calculates totals, preserves formatting (bold, merged cells, borders)
"""

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side


def generate_structure(input_file: str, output_file: str = "Структура.xlsx"):
    """
    Main function: Extract from "План" → Transform (aggregate all hours) → Load formatted "Структура"
    """

    # 1. Extract ALL hours from "План" (including non-theme rows)
    df_plan = pd.read_excel(input_file, sheet_name="План", header=None)

    # Initialize totals
    grand_total = 0
    grand_lectures = 0
    grand_practical = 0
    grand_self = 0

    sections = []
    current_section = None

    # ключ: (section, theme_name)
    themes = {}

    for _, row in df_plan.iterrows():
        label = str(row[0]).strip() if pd.notnull(row[0]) else ""

        # ---- РОЗДІЛ ----
        if label.startswith("РОЗДІЛ"):
            current_section = label
            sections.append(current_section)

        # ---- ТЕМА ----
        elif label.startswith("Тема") and current_section:
            theme_name = label

            total_hours = row[1] if pd.notnull(row[1]) else 0
            lectures = row[3] if pd.notnull(row[3]) else 0
            practical = row[4] if pd.notnull(row[4]) else 0
            self_work = row[5] if pd.notnull(row[5]) else 0

            key = (current_section, theme_name)

            if key not in themes:
                themes[key] = {
                    "section": current_section,
                    "theme": theme_name,
                    "total": 0,
                    "lectures": 0,
                    "practical": 0,
                    "lab": 0,
                    "individual": 0,
                    "self": 0
                }

            themes[key]["total"] += total_hours
            themes[key]["lectures"] += lectures
            themes[key]["practical"] += practical
            themes[key]["self"] += self_work

            # Додаємо до загальних підсумків
            grand_total += total_hours
            grand_lectures += lectures
            grand_practical += practical
            grand_self += self_work

    # 2. Transform → structure table
    structure_data = [
        ["Назви змістових модулів і тем", "Кількість годин", "", "", "", "", ""],
        ["", "денна форма", "", "", "", "", ""],
        ["", "усього", "у тому числі", "", "", "", ""],
        ["", "", "лекції", "практичні, семінарські", "лабораторні", "індивідуальні завдання", "самостійна робота"]
    ]

    for section in sections:
        structure_data.append([section, "", "", "", "", "", ""])

        section_themes = [t for t in themes.values() if t["section"] == section]

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

        # ---- Разом за розділом ----
        total = sum(t["total"] for t in section_themes)
        lectures = sum(t["lectures"] for t in section_themes)
        practical = sum(t["practical"] for t in section_themes)
        self_work = sum(t["self"] for t in section_themes)

        section_num = section.split()[1].rstrip('.')
        structure_data.append([
            f"Разом за розділом {section_num}",
            total, lectures, practical, 0, 0, self_work
        ])

    # ---- ВСЬОГО (використовуємо накопичені загальні суми) ----
    structure_data.append([
        "ВСЬОГО ПО НАВЧАЛЬНІЙ ДИСЦИПЛІНІ:",
        grand_total, grand_lectures, grand_practical, 0, 0, grand_self
    ])

    # 3. Load → Excel with formatting
    wb = Workbook()
    ws = wb.active
    ws.title = "Структура"


    # write data
    for r_idx, row in enumerate(structure_data, 1):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    # ---- Formatting ----
    bold_font = Font(bold=True)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    ws.merge_cells("B2:G2")
    ws.merge_cells("B3:B4")
    ws.merge_cells("A1:A4")
    ws.merge_cells("C2:G2")
    ws.merge_cells("C3:G3")
    ws.cell(2, 3).font = bold_font
    ws.cell(2, 3).alignment = center_align

    # header rows
    for row in ws.iter_rows(min_row=1, max_row=4):
        for cell in row:
            cell.font = bold_font
            cell.alignment = center_align

    # content rows
    for row in ws.iter_rows(min_row=5, max_row=ws.max_row):
        first_cell = row[0]

        if first_cell.value and (
            str(first_cell.value).startswith("РОЗДІЛ")
            or str(first_cell.value).startswith("Разом")
            or str(first_cell.value).startswith("ВСЬОГО")
        ):
            first_cell.font = bold_font

        for cell in row:
            if cell.column > 1:
                cell.alignment = center_align

    # auto width
    for col in ws.columns:
        max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_len + 2

    wb.save(output_file)
    print(f"Генерація завершена! Файл створено: {output_file}")


if __name__ == "__main__":
    generate_structure("НПр КН 2025.xlsx")
