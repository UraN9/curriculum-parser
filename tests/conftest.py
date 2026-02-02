import pytest
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set TEST_DATABASE_URL before importing app modules
# This ensures tests use isolated test database
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg2://postgres:root@localhost:5432/test_curriculum_db"
)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

from api import create_app
from app.database import Base, engine


@pytest.fixture(scope='session', autouse=True)
def setup_test_db():
    """
    Setup isolated test database.
    
    Creates all tables before tests, drops them after.
    Uses separate database (test_curriculum_db) to avoid affecting development data.
    """
    print(f"\n[TEST] Using test database: {TEST_DATABASE_URL}")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    yield
    
    # Clean up: drop all views and tables after tests
    with engine.connect() as conn:
        # Drop materialized views
        result = conn.execute(text(
            "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public';"
        ))
        for row in result:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {row[0]} CASCADE;"))
        
        # Drop regular views
        result = conn.execute(text(
            "SELECT table_name FROM information_schema.views WHERE table_schema = 'public';"
        ))
        for row in result:
            conn.execute(text(f"DROP VIEW IF EXISTS {row[0]} CASCADE;"))
        
        conn.commit()
    
    # Drop all tables
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def app():
    app = create_app()
    app.config['TESTING'] = True
    return app

@pytest.fixture
def client(app):
    return app.test_client()
