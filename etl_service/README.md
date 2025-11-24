# ETL / CDC Service

This module handles automated import and processing of teacher curriculum Excel files, as well as real-time tracking of database changes (CDC).

## Overview

The ETL/CDC service performs the following tasks:

* **One-time Excel import** – read the `Plan` sheet, validate data (hours ≥ 0, names filled, totals match).  
* **Transform & Aggregate** – compute sums, group data by semester and discipline.  
* **Load & Update Tables** – update database tables and log any errors in `etl_errors`.  
* **CDC (Change Data Capture)** – capture changes from the database in real-time using Debezium + WAL/binlog.  
* **Refresh Summary Tables** – update aggregated summary tables for quick access.

## Process Flow

```mermaid
flowchart TD
    A([Start ETL / CDC]) --> B{Source?}
    B -->|Excel| E[Extract: Read sheet Plan]
    B -->|Database| C[CDC: Capture changes<br/>Debezium + WAL/binlog]
    
    E --> V[Validate data<br/>Hours >= 0<br/>Names filled<br/>Totals match]
    C --> V
    
    V -->|Failed| Err[Log errors to etl_errors]
    V -->|Passed| T[Transform & aggregate<br/>SQL: GROUP BY + SUM]
    
    T --> L[Load/Update tables<br/>]
    L --> S[Refresh summary tables]
    
    S --> OK([Success])
    Err --> FAIL([Failed])
    C -->|No changes| OK

    style A fill:#90EE90,stroke:#27ae60
    style OK fill:#90EE90,stroke:#27ae60
    style FAIL fill:#FFB6C1,stroke:#e74c3c
    style Err fill:#FFB6C1,stroke:#e74c3c
    style C fill:#3498db,stroke:#2980b9,color:white
