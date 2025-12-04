# 🔌 ORM & Database

The project uses **SQLAlchemy 2.0** together with **PostgreSQL**.

---

## ✨ Features
- 🧩 **9 entities** fully modeled with proper relationships (`one-to-many`, `many-to-one`)
- 🔐 **Constraints**: PK, FK (`ondelete="CASCADE"`), `UNIQUE`, `ENUM` (role, weekday)
- 🛡️ **Secure configuration** via `.env`
- 🏗️ **Automatic table creation** on startup (idempotent)
- 🧪 **Test lecturer record** is inserted automatically to verify that database initialization works correctly

---

## 🚀 Quick verification

```bash
# 1. Copy environment template
cp .env.example .env

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ORM initialization
python -m app.main
```

![Python](https://img.shields.io/badge/Python-3.13.7-blue?style=flat&logo=python)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy_2.0-Initialized-lightgray?style=flat&logo=sqlalchemy)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Connected-brightgreen?style=flat&logo=postgresql)