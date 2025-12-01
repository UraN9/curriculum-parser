"""
Database connection configuration using SQLAlchemy 2.0

Loads configuration from .env file for security and flexibility.
Provides the engine, session factory, and base class for all models.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os

# Load environment variables from .env file (if present)
load_dotenv()

# Database URL — first tries to read from .env, otherwise uses default local PostgreSQL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:password@localhost:5432/curriculum_db"
)

# Create SQLAlchemy engine — the core interface to the database
engine = create_engine(
    DATABASE_URL,
    echo=False,      # Set to True to see all SQL statements (useful for debugging)
    future=True      # Required for SQLAlchemy 2.0+ features
)

# Session factory for database sessions
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True
)

# Base class for all ORM models
Base = declarative_base()


# Dependency for FastAPI — yields a database session and ensures proper cleanup
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()