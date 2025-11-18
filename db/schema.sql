BEGIN;

-- (Optional) Create a diagram if necessary
-- CREATE SCHEMA IF NOT EXISTS studies;
-- SET search_path = studies, public;

-- Lookup tables
CREATE TABLE activity_type (
    id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE control_form (
    id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Main tables
CREATE TABLE lecturer (
    id            INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name     VARCHAR(200) NOT NULL,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(20) NOT NULL CHECK (role IN ('admin','lecturer','viewer'))
);

CREATE TABLE discipline (
    id           INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    course       INT CHECK (course > 0),
    ects_credits NUMERIC(4,2) CHECK (ects_credits >= 0),
    lecturer_id  INT,
    CONSTRAINT fk_discipline_lecturer FOREIGN KEY (lecturer_id)
        REFERENCES lecturer (id) ON UPDATE CASCADE
);

CREATE TABLE semester (
    id             INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    number         INT NOT NULL CHECK (number > 0),
    weeks          INT NOT NULL CHECK (weeks > 0),
    hours_per_week INT NOT NULL CHECK (hours_per_week >= 0)
);

CREATE TABLE section (
    id            INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          VARCHAR(200) NOT NULL,
    discipline_id INT NOT NULL,
    semester_id   INT NOT NULL,
    CONSTRAINT fk_section_discipline FOREIGN KEY (discipline_id)
        REFERENCES discipline (id) ON UPDATE CASCADE,
    CONSTRAINT fk_section_semester FOREIGN KEY (semester_id)
        REFERENCES semester (id) ON UPDATE CASCADE
);

CREATE TABLE theme (
    id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    section_id  INT NOT NULL,
    total_hours INT DEFAULT 0 CHECK (total_hours >= 0),
    CONSTRAINT fk_theme_section FOREIGN KEY (section_id)
        REFERENCES section (id) ON UPDATE CASCADE
);

CREATE TABLE activity (
    id               INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name             VARCHAR(200) NOT NULL,
    type_id          INT NOT NULL,
    hours            INT DEFAULT 0 CHECK (hours >= 0),
    theme_id         INT NOT NULL,
    control_form_id  INT NOT NULL,
    CONSTRAINT fk_activity_type FOREIGN KEY (type_id)
        REFERENCES activity_type (id) ON UPDATE CASCADE,
    CONSTRAINT fk_activity_theme FOREIGN KEY (theme_id)
        REFERENCES theme (id) ON UPDATE CASCADE,
    CONSTRAINT fk_activity_control FOREIGN KEY (control_form_id)
        REFERENCES control_form (id) ON UPDATE CASCADE
);

CREATE TYPE weekday AS ENUM (
    'monday','tuesday','wednesday','thursday','friday','saturday','sunday'
);

CREATE TABLE schedule (
    id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    day         weekday NOT NULL,
    pair_number INT NOT NULL CHECK (pair_number > 0),
    room        VARCHAR(100),
    activity_id INT NOT NULL,
    CONSTRAINT fk_schedule_activity FOREIGN KEY (activity_id)
        REFERENCES activity (id) ON UPDATE CASCADE
);

CREATE INDEX idx_discipline_lecturer_id ON discipline(lecturer_id);
CREATE INDEX idx_section_discipline_id ON section(discipline_id);
CREATE INDEX idx_section_semester_id ON section(semester_id);
CREATE INDEX idx_theme_section_id ON theme(section_id);
CREATE INDEX idx_activity_theme_id ON activity(theme_id);
CREATE INDEX idx_activity_type_id ON activity(type_id);
CREATE INDEX idx_activity_control_form_id ON activity(control_form_id);
CREATE INDEX idx_schedule_activity_id ON schedule(activity_id);

COMMIT;
