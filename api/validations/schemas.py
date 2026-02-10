"""
Marshmallow schemas for API validation.

These schemas validate input data before saving to database.
Returns HTTP 422 for validation errors.
"""
from marshmallow import Schema, fields, validate, validates, ValidationError, post_load
from .validators import (
    validate_name,
    validate_email,
    validate_full_name,
    validate_course,
    validate_ects_credits,
    validate_hours,
    validate_positive_integer
)


class LecturerSchema(Schema):
    """Schema for Lecturer validation."""
    
    id = fields.Integer(dump_only=True)
    full_name = fields.String(required=True)
    email = fields.String(required=True)
    password = fields.String(load_only=True, required=True)
    role = fields.String(
        validate=validate.OneOf(['admin', 'lecturer', 'viewer']),
        load_default='lecturer'
    )
    
    @validates('full_name')
    def validate_full_name_field(self, value, **kwargs):
        validate_full_name(value)
    
    @validates('email')
    def validate_email_field(self, value, **kwargs):
        validate_email(value)
    
    @validates('password')
    def validate_password(self, value, **kwargs):
        if not value or len(value) < 6:
            raise ValidationError("Password must be at least 6 characters long.")


class DisciplineSchema(Schema):
    """
    Schema for Discipline validation.
    
    Validates:
    - name: required, 2-200 chars, valid characters
    - course: required, integer 1-6
    - ects_credits: required, 0.5-30.0
    - lecturer_id: required, positive integer (FK)
    """
    
    id = fields.Integer(dump_only=True)
    name = fields.String(required=True)
    course = fields.Integer(required=True)
    ects_credits = fields.Float(required=True)
    lecturer_id = fields.Integer(required=True)
    
    @validates('name')
    def validate_name_field(self, value, **kwargs):
        validate_name(value)
    
    @validates('course')
    def validate_course_field(self, value, **kwargs):
        validate_course(value)
    
    @validates('ects_credits')
    def validate_ects_field(self, value, **kwargs):
        validate_ects_credits(value)
    
    @validates('lecturer_id')
    def validate_lecturer_id_field(self, value, **kwargs):
        validate_positive_integer(value, "Lecturer ID")


class SectionSchema(Schema):
    """
    Schema for Section validation.
    
    Validates:
    - name: required, 2-200 chars
    - discipline_id: required, positive integer (FK)
    - semester_id: optional, positive integer (FK)
    """
    
    id = fields.Integer(dump_only=True)
    name = fields.String(required=True)
    discipline_id = fields.Integer(required=True)
    semester_id = fields.Integer(allow_none=True)
    
    @validates('name')
    def validate_name_field(self, value, **kwargs):
        validate_name(value)
    
    @validates('discipline_id')
    def validate_discipline_id_field(self, value, **kwargs):
        validate_positive_integer(value, "Discipline ID")
    
    @validates('semester_id')
    def validate_semester_id_field(self, value, **kwargs):
        if value is not None:
            validate_positive_integer(value, "Semester ID")


class ThemeSchema(Schema):
    """
    Schema for Theme validation.
    
    Validates:
    - name: required, 2-200 chars
    - section_id: required, positive integer (FK)
    """
    
    id = fields.Integer(dump_only=True)
    name = fields.String(required=True)
    section_id = fields.Integer(required=True)
    
    @validates('name')
    def validate_name_field(self, value, **kwargs):
        validate_name(value)
    
    @validates('section_id')
    def validate_section_id_field(self, value, **kwargs):
        validate_positive_integer(value, "Section ID")


class ActivitySchema(Schema):
    """
    Schema for Activity validation.
    
    Validates:
    - hours: required, 1-100
    - theme_id: required, positive integer (FK)
    - activity_type_id: required, positive integer (FK)
    """
    
    id = fields.Integer(dump_only=True)
    hours = fields.Integer(required=True)
    theme_id = fields.Integer(required=True)
    activity_type_id = fields.Integer(required=True)
    
    @validates('hours')
    def validate_hours_field(self, value, **kwargs):
        validate_hours(value)
    
    @validates('theme_id')
    def validate_theme_id_field(self, value, **kwargs):
        validate_positive_integer(value, "Theme ID")
    
    @validates('activity_type_id')
    def validate_activity_type_id_field(self, value, **kwargs):
        validate_positive_integer(value, "Activity Type ID")


class SemesterSchema(Schema):
    """
    Schema for Semester validation.
    
    Validates:
    - number: required, 1-12
    - weeks: required, 1-20
    - hours_per_week: required, 1-40
    """
    
    id = fields.Integer(dump_only=True)
    number = fields.Integer(required=True)
    weeks = fields.Integer(required=True)
    hours_per_week = fields.Integer(required=True)
    
    @validates('number')
    def validate_number_field(self, value, **kwargs):
        if not isinstance(value, int) or value < 1 or value > 12:
            raise ValidationError("Semester number must be between 1 and 12.")
    
    @validates('weeks')
    def validate_weeks_field(self, value, **kwargs):
        if not isinstance(value, int) or value < 1 or value > 20:
            raise ValidationError("Weeks must be between 1 and 20.")
    
    @validates('hours_per_week')
    def validate_hours_per_week_field(self, value, **kwargs):
        if not isinstance(value, int) or value < 1 or value > 40:
            raise ValidationError("Hours per week must be between 1 and 40.")
