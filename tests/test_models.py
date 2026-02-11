import pytest
from sqlalchemy.orm import Session
from app.models import Lecturer, Student, RoleEnum
from app.database import engine

@pytest.fixture(scope="function")
def db_session():
    session = Session(bind=engine)
    # Clean up tables before each test
    session.query(Lecturer).delete()
    session.query(Student).delete()
    session.commit()
    yield session
    session.close()

def test_create_lecturer(db_session):
    lecturer = Lecturer(
        full_name="Test Lecturer",
        email="lecturer@test.ua",
        password_hash="hash123",
        role=RoleEnum.admin
    )
    db_session.add(lecturer)
    db_session.commit()
    found = db_session.query(Lecturer).filter_by(email="lecturer@test.ua").first()
    assert found is not None
    assert found.role == RoleEnum.admin

def test_create_student(db_session):
    student = Student(
        full_name="Test Student",
        email="student@test.ua",
        password_hash="hash456",
        role=RoleEnum.viewer
    )
    db_session.add(student)
    db_session.commit()
    found = db_session.query(Student).filter_by(email="student@test.ua").first()
    assert found is not None
    assert found.role == RoleEnum.viewer
