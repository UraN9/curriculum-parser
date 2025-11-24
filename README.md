# Curriculum Parser Project
Automated ETL system for parsing teacher curriculum Excel files and validating data.

---

## Project Overview

This project includes:

1. **PostgreSQL Database** – physical data schema storing all curriculum-related information.
2. **ETL + CDC Pipeline** – automated import and validation of teacher curriculum Excel files with one-time loading, data transformation and aggregation, as well as real-time tracking of database changes through CDC (Debezium + WAL/binlog).Documentation and UML diagram are available in the [ETL/CDC Service](./etl_service/README.md).

## Database Schema

This section describes the **physical database schema** for the diploma project, including ER-diagram, entities, attributes, relations, data types, and constraints.

## ER Diagram

```mermaid
---
config:
  layout: dagre
  theme: redux-color
---
erDiagram
    LECTURER ||--o{ DISCIPLINE : "teaches<br>1 → N"
    DISCIPLINE ||--o{ SEMESTER : "has<br>1 → N"
    DISCIPLINE ||--o{ SECTION : "contains<br>1 → N"
    SECTION ||--o{ THEME : "includes<br>1 → N"
    THEME ||--o{ ACTIVITY : "consists_of<br>1 → N"
    ACTIVITY }o--|| ACTIVITY_TYPE : "is_of_type<br>N → 1"
    ACTIVITY }o--|| CONTROL_FORM : "has_control<br>N → 1"
    ACTIVITY ||--o{ SCHEDULE : "scheduled_in<br>1 → N"
    LECTURER {
        int id PK
        string full_name
        string email_UNIQUE
        string password_hash
        string role
    }
    DISCIPLINE {
        int id PK
        string name
        int course
        float ects_credits
        int lecturer_id FK
    }
    SEMESTER {
        int id PK
        int number
        int weeks
        int hours_per_week
    }
    SECTION {
        int id PK
        string name
        int discipline_id FK
        int semester_id FK
    }
    THEME {
        int id PK
        string name
        int section_id FK
        int total_hours
    }
    ACTIVITY {
        int id PK
        string name
        int type_id FK
        int hours
        int theme_id FK
        int control_form_id FK
    }
    ACTIVITY_TYPE {
        int id PK
        string name
    }
    CONTROL_FORM {
        int id PK
        string name
    }
    SCHEDULE {
        int id PK
        string day
        int pair_number
        string room
        int activity_id FK
    }

```

---

## Entities

* LECTURER
* DISCIPLINE
* SEMESTER
* SECTION
* THEME
* ACTIVITY
* ACTIVITY_TYPE
* CONTROL_FORM
* SCHEDULE

---

## Attributes

**LECTURER**: `id`, `full_name`, `email`, `password_hash`, `role`<br>
**DISCIPLINE**: `id`, `name`, `course`, `ects_credits`, `lecturer_id`<br>
**SEMESTER**: `id`, `number`, `weeks`, `hours_per_week`<br>
**SECTION**: `id`, `name`, `discipline_id`, `semester_id`<br>
**THEME**: `id`, `name`, `section_id`, `total_hours`<br>
**ACTIVITY**: `id`, `name`, `type_id`, `hours`, `theme_id`, `control_form_id`<br>
**ACTIVITY_TYPE**: `id`, `name`<br>
**CONTROL_FORM**: `id`, `name`<br>
**SCHEDULE**: `id`, `day`, `pair_number`, `room`, `activity_id`


---

## Relations

| Entity 1   | Relationship | Entity 2      | Type  |
| ---------- | ------------ | ------------- | ----- |
| LECTURER   | teaches      | DISCIPLINE    | 1 → N |
| DISCIPLINE | has          | SEMESTER      | 1 → N |
| DISCIPLINE | contains     | SECTION       | 1 → N |
| SECTION    | includes     | THEME         | 1 → N |
| THEME      | consists_of  | ACTIVITY      | 1 → N |
| ACTIVITY   | is_of_type   | ACTIVITY_TYPE | N → 1 |
| ACTIVITY   | has_control  | CONTROL_FORM  | N → 1 |
| ACTIVITY   | scheduled_in | SCHEDULE      | 1 → N |

---

## Data Types

| Attribute       | Type         |
| --------------- | ------------ |
| id              | INT          |
| full_name       | VARCHAR      |
| email           | VARCHAR      |
| password_hash   | VARCHAR      |
| role            | VARCHAR      |
| name            | VARCHAR      |
| course          | INT          |
| ects_credits    | NUMERIC      |
| lecturer_id     | INT          |
| number          | INT          |
| weeks           | INT          |
| hours_per_week  | INT          |
| discipline_id   | INT          |
| semester_id     | INT          |
| section_id      | INT          |
| total_hours     | INT          |
| type_id         | INT          |
| hours           | INT          |
| theme_id        | INT          |
| control_form_id | INT          |
| day             | weekday ENUM |
| pair_number     | INT          |
| room            | VARCHAR      |
| activity_id     | INT          |

---

## Constraints

* Primary Keys: `id` fields in all tables
* Unique: `LECTURER.email`, `ACTIVITY_TYPE.name`, `CONTROL_FORM.name`
* Foreign Keys:

  * `DISCIPLINE.lecturer_id → LECTURER.id`
  * `SECTION.discipline_id → DISCIPLINE.id`
  * `SECTION.semester_id → SEMESTER.id`
  * `THEME.section_id → SECTION.id`
  * `ACTIVITY.type_id → ACTIVITY_TYPE.id`
  * `ACTIVITY.theme_id → THEME.id`
  * `ACTIVITY.control_form_id → CONTROL_FORM.id`
  * `SCHEDULE.activity_id → ACTIVITY.id`
* Checks:

  * `LECTURER.role` IN ('admin','lecturer','viewer')
  * Numeric fields ≥ 0 where applicable

* [Database Schema SQL](db/schema.sql)

---

**Notes:**

* This documentation serves as a centralized reference for project development.

---