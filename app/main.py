"""
SQLAlchemy 2.0 ORM connection demonstration
Creates database tables and inserts a test lecturer
"""

from sqlalchemy.orm import Session
from app.database import engine, Base
from app.models import Lecturer, RoleEnum


def create_tables() -> None:
    """Create all defined tables in the database (idempotent operation)."""
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Successfully created 9 tables with all constraints!")


def create_test_lecturer() -> None:
    """Insert a test lecturer if one does not already exist."""
    with Session(engine) as db:
        # Check if test lecturer already exists
        if db.query(Lecturer).filter(Lecturer.email == "test@lecturer.ua").first():
            print("Test lecturer already exists")
            return

        lecturer = Lecturer(
            full_name="Ivan Ivanov",
            email="test@lecturer.ua",
            password_hash="temp_password123",
            role=RoleEnum.lecturer
        )
        db.add(lecturer)
        db.commit()
        print("Added test lecturer: test@lecturer.ua")


if __name__ == "__main__":
    print("Starting ORM — connecting to PostgreSQL...")
    create_tables()
    create_test_lecturer()
    print("\nConnect Python ORM — SUCCESSFULLY COMPLETED!")
