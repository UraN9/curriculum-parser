import pytest
from api import create_app
from app.database import Base, engine
from sqlalchemy import text


@pytest.fixture(scope='session', autouse=True)
def setup_test_db():
    # Create all tables in the test database before running tests
    Base.metadata.create_all(bind=engine)
    yield
    # Drop all materialized views and views before dropping tables
    with engine.connect() as conn:
        # Drop materialized views
        result = conn.execute(text("""
            SELECT matviewname FROM pg_matviews WHERE schemaname = 'public';
        """))
        for row in result:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {row[0]} CASCADE;"))
        # Drop regular views
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.views WHERE table_schema = 'public';
        """))
        for row in result:
            conn.execute(text(f"DROP VIEW IF EXISTS {row[0]} CASCADE;"))
        conn.commit()
    # Drop all tables after tests are finished (clean up the test database)
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def app():
    app = create_app()
    app.config['TESTING'] = True
    return app

@pytest.fixture
def client(app):
    return app.test_client()
