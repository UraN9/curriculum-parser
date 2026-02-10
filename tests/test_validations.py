"""
Unit tests for data integrity validations.

Tests validation schemas and regex patterns for:
- Discipline
- Section  
- Theme
- Activity
- Lecturer
"""
import pytest
from marshmallow import ValidationError
from api.validations.schemas import (
    DisciplineSchema,
    SectionSchema,
    ThemeSchema,
    ActivitySchema,
    LecturerSchema,
    SemesterSchema
)
from api.validations.validators import (
    validate_name,
    validate_email,
    validate_full_name,
    validate_course,
    validate_ects_credits,
    validate_hours,
    validate_positive_integer,
    validate_ukrainian_text
)


class TestValidatorFunctions:
    """Test individual validator functions."""
    
    # ========== validate_name ==========
    
    def test_validate_name_valid(self):
        """Valid names should pass."""
        valid_names = [
            "Бази даних",
            "Програмування",
            "Web-технології",
            "Python 3.0",
            "Тема №1",
            "Об'єктно-орієнтоване програмування"
        ]
        for name in valid_names:
            validate_name(name)  # Should not raise
    
    def test_validate_name_empty(self):
        """Empty name should fail."""
        with pytest.raises(ValidationError) as exc:
            validate_name("")
        assert "empty" in str(exc.value).lower()
    
    def test_validate_name_too_short(self):
        """Name with 1 character should fail."""
        with pytest.raises(ValidationError) as exc:
            validate_name("A")
        assert "2 characters" in str(exc.value)
    
    def test_validate_name_too_long(self):
        """Name over 200 characters should fail."""
        long_name = "А" * 201
        with pytest.raises(ValidationError) as exc:
            validate_name(long_name)
        assert "200" in str(exc.value)
    
    # ========== validate_email ==========
    
    def test_validate_email_valid(self):
        """Valid emails should pass."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.org",
            "admin123@company.co.ua"
        ]
        for email in valid_emails:
            validate_email(email)  # Should not raise
    
    def test_validate_email_invalid(self):
        """Invalid emails should fail."""
        invalid_emails = [
            "notanemail",
            "missing@domain",
            "@nodomain.com",
            "spaces in@email.com"
        ]
        for email in invalid_emails:
            with pytest.raises(ValidationError):
                validate_email(email)
    
    # ========== validate_full_name ==========
    
    def test_validate_full_name_valid(self):
        """Valid Ukrainian names should pass."""
        valid_names = [
            "Іванов Іван Іванович",
            "Петренко-Коваль Марія",
            "О'Коннор Патрік"
        ]
        for name in valid_names:
            validate_full_name(name)  # Should not raise
    
    def test_validate_full_name_with_digits(self):
        """Names with digits should fail."""
        with pytest.raises(ValidationError):
            validate_full_name("Іванов123")
    
    # ========== validate_course ==========
    
    def test_validate_course_valid(self):
        """Valid course numbers 1-6 should pass."""
        for course in range(1, 7):
            validate_course(course)  # Should not raise
    
    def test_validate_course_zero(self):
        """Course 0 should fail."""
        with pytest.raises(ValidationError) as exc:
            validate_course(0)
        assert "between 1 and 6" in str(exc.value)
    
    def test_validate_course_too_high(self):
        """Course > 6 should fail."""
        with pytest.raises(ValidationError) as exc:
            validate_course(7)
        assert "between 1 and 6" in str(exc.value)
    
    def test_validate_course_not_integer(self):
        """Non-integer course should fail."""
        with pytest.raises(ValidationError):
            validate_course("3")
    
    # ========== validate_ects_credits ==========
    
    def test_validate_ects_valid(self):
        """Valid ECTS credits should pass."""
        valid_credits = [0.5, 1.0, 5.0, 15.5, 30.0]
        for credits in valid_credits:
            validate_ects_credits(credits)  # Should not raise
    
    def test_validate_ects_too_low(self):
        """Credits < 0.5 should fail."""
        with pytest.raises(ValidationError):
            validate_ects_credits(0.1)
    
    def test_validate_ects_too_high(self):
        """Credits > 30 should fail."""
        with pytest.raises(ValidationError):
            validate_ects_credits(31.0)
    
    # ========== validate_hours ==========
    
    def test_validate_hours_valid(self):
        """Valid hours should pass."""
        for hours in [1, 2, 50, 100]:
            validate_hours(hours)  # Should not raise
    
    def test_validate_hours_zero(self):
        """Zero hours should fail."""
        with pytest.raises(ValidationError):
            validate_hours(0)
    
    def test_validate_hours_too_high(self):
        """Hours > 100 should fail."""
        with pytest.raises(ValidationError):
            validate_hours(101)


class TestDisciplineSchema:
    """Test DisciplineSchema validation."""
    
    def setup_method(self):
        self.schema = DisciplineSchema()
    
    def test_valid_discipline(self):
        """Valid discipline data should pass."""
        data = {
            'name': 'Бази даних',
            'course': 3,
            'ects_credits': 5.0,
            'lecturer_id': 1
        }
        result = self.schema.load(data)
        assert result['name'] == 'Бази даних'
        assert result['course'] == 3
    
    def test_missing_required_field(self):
        """Missing required field should fail with 422."""
        data = {
            'name': 'Бази даних',
            'course': 3
            # Missing ects_credits and lecturer_id
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'ects_credits' in exc.value.messages
        assert 'lecturer_id' in exc.value.messages
    
    def test_invalid_course(self):
        """Invalid course should fail."""
        data = {
            'name': 'Бази даних',
            'course': 10,  # Invalid: > 6
            'ects_credits': 5.0,
            'lecturer_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'course' in exc.value.messages
    
    def test_invalid_name_empty(self):
        """Empty name should fail."""
        data = {
            'name': '',
            'course': 3,
            'ects_credits': 5.0,
            'lecturer_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'name' in exc.value.messages
    
    def test_invalid_ects_negative(self):
        """Negative ECTS should fail."""
        data = {
            'name': 'Бази даних',
            'course': 3,
            'ects_credits': -1.0,
            'lecturer_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'ects_credits' in exc.value.messages


class TestSectionSchema:
    """Test SectionSchema validation."""
    
    def setup_method(self):
        self.schema = SectionSchema()
    
    def test_valid_section(self):
        """Valid section data should pass."""
        data = {
            'name': 'Розділ 1: Вступ',
            'discipline_id': 1
        }
        result = self.schema.load(data)
        assert result['name'] == 'Розділ 1: Вступ'
    
    def test_missing_discipline_id(self):
        """Missing discipline_id should fail."""
        data = {'name': 'Розділ 1'}
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'discipline_id' in exc.value.messages


class TestThemeSchema:
    """Test ThemeSchema validation."""
    
    def setup_method(self):
        self.schema = ThemeSchema()
    
    def test_valid_theme(self):
        """Valid theme data should pass."""
        data = {
            'name': 'Тема 1: Реляційні бази даних',
            'section_id': 1
        }
        result = self.schema.load(data)
        assert result['name'] == 'Тема 1: Реляційні бази даних'
    
    def test_name_too_short(self):
        """Name with 1 character should fail."""
        data = {
            'name': 'A',
            'section_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'name' in exc.value.messages


class TestActivitySchema:
    """Test ActivitySchema validation."""
    
    def setup_method(self):
        self.schema = ActivitySchema()
    
    def test_valid_activity(self):
        """Valid activity data should pass."""
        data = {
            'hours': 2,
            'theme_id': 1,
            'activity_type_id': 1
        }
        result = self.schema.load(data)
        assert result['hours'] == 2
    
    def test_invalid_hours_zero(self):
        """Zero hours should fail."""
        data = {
            'hours': 0,
            'theme_id': 1,
            'activity_type_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'hours' in exc.value.messages
    
    def test_invalid_hours_too_high(self):
        """Hours > 100 should fail."""
        data = {
            'hours': 150,
            'theme_id': 1,
            'activity_type_id': 1
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'hours' in exc.value.messages


class TestLecturerSchema:
    """Test LecturerSchema validation."""
    
    def setup_method(self):
        self.schema = LecturerSchema()
    
    def test_valid_lecturer(self):
        """Valid lecturer data should pass."""
        data = {
            'full_name': 'Іванов Іван Іванович',
            'email': 'ivanov@example.com',
            'password': 'securepass123'
        }
        result = self.schema.load(data)
        assert result['full_name'] == 'Іванов Іван Іванович'
    
    def test_invalid_email(self):
        """Invalid email should fail."""
        data = {
            'full_name': 'Іванов Іван',
            'email': 'not-an-email',
            'password': 'securepass123'
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'email' in exc.value.messages
    
    def test_password_too_short(self):
        """Password < 6 chars should fail."""
        data = {
            'full_name': 'Іванов Іван',
            'email': 'test@example.com',
            'password': '123'
        }
        with pytest.raises(ValidationError) as exc:
            self.schema.load(data)
        assert 'password' in exc.value.messages
