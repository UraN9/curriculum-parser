# ⚡ ETL / CDC Service

This module handles automated import and processing of teacher curriculum Excel files, as well as real-time tracking of database changes (CDC).

## 🚀 Overview

The ETL/CDC service performs the following tasks:

* 📥 **One-time Excel import** – read the `Plan` sheet, validate data (hours ≥ 0, names filled, totals match).  
* ✅ **Comprehensive Validation** – structured validation with error/warning categorization and detailed reporting.
* 🔄 **Transform & Aggregate** – compute sums, group data by semester and discipline.  
* 💾  **Load & Update Tables** – update database tables and log any errors in `etl_errors`.  
* 🟢 **CDC (Change Data Capture)** – capture changes from the database in real-time using Debezium + WAL/binlog.  
* 📊 **Refresh Summary Tables** – update aggregated summary tables for quick access.

## ✅ Validation Module

The `validation.py` module provides comprehensive data validation with the following features:

- **Error vs Warning categorization**: Critical errors block processing, warnings are informational
- **Structured validation reports**: Human-readable output with issue details and row numbers
- **Multiple check types**:
  - Negative hours detection
  - Empty required fields validation
  - Hour total consistency verification
  - Non-numeric value detection
- **Extensible design**: Easy to add new validation rules

### Usage Example

```python
from validation import validate_plan_data, format_validation_report
import pandas as pd

df = pd.read_excel("input.xlsx", sheet_name="План", header=None)
result = validate_plan_data(df)
print(format_validation_report(result))

if result.is_valid:
    print("✓ Validation passed - ready for ETL processing")
else:
    print(f"✗ Found {result.error_count} critical error(s)")
```

## 🏭 Process Flow

```mermaid
flowchart TD
    A([Start ETL / CDC]) --> B{Source?}
    B -->|Excel| E[Extract: Read sheet Plan]
    B -->|Database| C[CDC: Capture changes<br/>Debezium + WAL/binlog]
    
    E --> V[Validate data<br/>Hours >= 0<br/>Names filled<br/>Totals match]
    C --> V
    
    V -->|Failed| Err[Log errors to etl_errors]
    V -->|Passed| T[Transform & aggregate<br/>SQL: GROUP BY + SUM]
    
    T --> L[Load/Update tables<br/>themes, activities, sections]
    L --> S[Refresh summary tables]
    
    S --> OK([Success])
    Err --> FAIL([Failed])
    C -->|No changes| OK

    style A fill:#27ae60,stroke:#1e8449,stroke-width:3px,color:white
    style OK fill:#27ae60,stroke:#1e8449,stroke-width:3px,color:white
    style FAIL fill:#e74c3c,stroke:#c0392b,stroke-width:3px,color:white
    style Err fill:#e67e22,stroke:#d35400,stroke-width:2px,color:white
    style C fill:#3498db,stroke:#2980b9,stroke-width:2px,color:white
