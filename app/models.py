"""
SQLAlchemy 2.0 ORM models for the curriculum management system
Contains all 9 entities with relationships and constraints
"""

from sqlalchemy import Column, Integer, String, Numeric, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from .database import Base
import enum


# Enums for constrained values
class RoleEnum(str, enum.Enum):
    admin = "admin"
    lecturer = "lecturer"
    viewer = "viewer"


class WeekdayEnum(str, enum.Enum):
    monday = "monday"
    tuesday = "tuesday"
    wednesday = "wednesday"
    thursday = "thursday"
    friday = "friday"


# 1. Lecturer
class Lecturer(Base):
    __tablename__ = "lecturers"

    id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False)
    email = Column(String(120), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(SQLEnum(RoleEnum), nullable=False, default=RoleEnum.lecturer)

    disciplines = relationship("Discipline", back_populates="lecturer")


# 2. Discipline (Course)
class Discipline(Base):
    __tablename__ = "disciplines"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    course = Column(Integer, nullable=False)
    ects_credits = Column(Numeric(4, 1), nullable=False)
    lecturer_id = Column(Integer, ForeignKey("lecturers.id", ondelete="CASCADE"), nullable=False)

    lecturer = relationship("Lecturer", back_populates="disciplines")
    sections = relationship("Section", back_populates="discipline")


# 3. Semester
class Semester(Base):
    __tablename__ = "semesters"

    id = Column(Integer, primary_key=True)
    number = Column(Integer, nullable=False)
    weeks = Column(Integer, nullable=False)
    hours_per_week = Column(Integer, nullable=False)


# 4. Section (Module/Unit within a discipline)
class Section(Base):
    __tablename__ = "sections"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    discipline_id = Column(Integer, ForeignKey("disciplines.id", ondelete="CASCADE"), nullable=False)
    semester_id = Column(Integer, ForeignKey("semesters.id"))

    discipline = relationship("Discipline", back_populates="sections")
    themes = relationship("Theme", back_populates="section")


# 5. Theme (Topic)
class Theme(Base):
    __tablename__ = "themes"

    id = Column(Integer, primary_key=True)
    name = Column(String(300), nullable=False)
    section_id = Column(Integer, ForeignKey("sections.id", ondelete="CASCADE"), nullable=False)
    total_hours = Column(Integer, nullable=False, default=0)

    section = relationship("Section", back_populates="themes")
    activities = relationship("Activity", back_populates="theme")


# 6. Activity Type (lecture, practice, lab, etc.)
class ActivityType(Base):
    __tablename__ = "activity_types"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)


# 7. Control Form (exam, credit, project, etc.)
class ControlForm(Base):
    __tablename__ = "control_forms"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)


# 8. Activity (specific class/session)
class Activity(Base):
    __tablename__ = "activities"

    id = Column(Integer, primary_key=True)
    name = Column(String(300))
    type_id = Column(Integer, ForeignKey("activity_types.id"))
    hours = Column(Integer, nullable=False, default=0)
    theme_id = Column(Integer, ForeignKey("themes.id", ondelete="CASCADE"), nullable=False)
    control_form_id = Column(Integer, ForeignKey("control_forms.id"))

    theme = relationship("Theme", back_populates="activities")
    type = relationship("ActivityType")
    control_form = relationship("ControlForm")


# 9. Schedule (timetable entry)
class Schedule(Base):
    __tablename__ = "schedule"

    id = Column(Integer, primary_key=True)
    day = Column(SQLEnum(WeekdayEnum), nullable=False)
    pair_number = Column(Integer, nullable=False)
    room = Column(String(20))
    activity_id = Column(Integer, ForeignKey("activities.id", ondelete="CASCADE"), unique=True, nullable=False)