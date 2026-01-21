# 🔌 ORM & Database

The project uses **SQLAlchemy 2.0** together with **PostgreSQL 18**.

![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red?style=flat&logo=sqlalchemy)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat&logo=postgresql)

---

## ✨ Features
- 🧩 **9 entities** fully modeled with proper relationships (`one-to-many`, `many-to-one`)
- 🔐 **Constraints**: PK, FK (`ondelete="CASCADE"`), `UNIQUE`, `ENUM` (role, weekday)
- 🛡️ **Secure configuration** via `.env`
- 🏗️ **Automatic table creation** on startup

---

## 📊 Database Tables

| Table | Description |
|-------|-------------|
| `lecturers` | Teachers/instructors |
| `disciplines` | Courses/subjects |
| `semesters` | Academic semesters |
| `sections` | Course sections/modules |
| `themes` | Topics within sections |
| `activities` | Learning activities (lectures, labs, etc.) |
| `activity_types` | Types of activities |
| `control_forms` | Assessment methods |
| `schedules` | Class schedules |
| `etl_errors` | ETL error logging |
| `change_log` | CDC audit trail |

---

## 🚀 Quick Start

```bash
# 1. Copy environment template
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ORM initialization
python -m app.main
```

---

## 📁 Files

| File | Description |
|------|-------------|
| `models.py` | SQLAlchemy ORM models |
| `database.py` | Database connection & session |
| `main.py` | Table initialization |