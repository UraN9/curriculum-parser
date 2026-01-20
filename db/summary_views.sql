-- ============================================================================
-- Materialized Views for Curriculum Summary Reports
-- ============================================================================
-- These views provide pre-aggregated data for fast reporting.
-- Refresh after ETL load using: SELECT refresh_all_summaries();
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. Summary by Section (Розділ)
-- ============================================================================
-- Aggregates hours by section with breakdown by activity type

DROP MATERIALIZED VIEW IF EXISTS mv_section_summary CASCADE;

CREATE MATERIALIZED VIEW mv_section_summary AS
SELECT 
    sec.id AS section_id,
    sec.name AS section_name,
    sem.id AS semester_id,
    sem.number AS semester_number,
    d.id AS discipline_id,
    d.name AS discipline_name,
    COUNT(DISTINCT t.id) AS theme_count,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS total_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лекція' THEN a.hours ELSE 0 END), 0) AS lecture_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Практична' THEN a.hours ELSE 0 END), 0) AS practical_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лабораторна' THEN a.hours ELSE 0 END), 0) AS lab_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Самостійна' THEN a.hours ELSE 0 END), 0) AS self_study_hours
FROM sections sec
LEFT JOIN semesters sem ON sec.semester_id = sem.id
LEFT JOIN disciplines d ON sec.discipline_id = d.id
LEFT JOIN themes t ON t.section_id = sec.id
LEFT JOIN activities a ON a.theme_id = t.id
LEFT JOIN activity_types at ON a.type_id = at.id
GROUP BY sec.id, sec.name, sem.id, sem.number, d.id, d.name
ORDER BY sem.number, sec.id;

-- Index for fast lookups
CREATE UNIQUE INDEX idx_mv_section_summary_id ON mv_section_summary(section_id);
CREATE INDEX idx_mv_section_summary_semester ON mv_section_summary(semester_id);
CREATE INDEX idx_mv_section_summary_discipline ON mv_section_summary(discipline_id);


-- ============================================================================
-- 2. Summary by Theme (Тема)
-- ============================================================================
-- Detailed hours breakdown per theme

DROP MATERIALIZED VIEW IF EXISTS mv_theme_summary CASCADE;

CREATE MATERIALIZED VIEW mv_theme_summary AS
SELECT 
    t.id AS theme_id,
    t.name AS theme_name,
    t.total_hours AS stored_total_hours,
    sec.id AS section_id,
    sec.name AS section_name,
    sem.number AS semester_number,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS calculated_total_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лекція' THEN a.hours ELSE 0 END), 0) AS lecture_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Практична' THEN a.hours ELSE 0 END), 0) AS practical_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лабораторна' THEN a.hours ELSE 0 END), 0) AS lab_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Самостійна' THEN a.hours ELSE 0 END), 0) AS self_study_hours
FROM themes t
LEFT JOIN sections sec ON t.section_id = sec.id
LEFT JOIN semesters sem ON sec.semester_id = sem.id
LEFT JOIN activities a ON a.theme_id = t.id
LEFT JOIN activity_types at ON a.type_id = at.id
GROUP BY t.id, t.name, t.total_hours, sec.id, sec.name, sem.number
ORDER BY sem.number, sec.id, t.id;

-- Index for fast lookups
CREATE UNIQUE INDEX idx_mv_theme_summary_id ON mv_theme_summary(theme_id);
CREATE INDEX idx_mv_theme_summary_section ON mv_theme_summary(section_id);


-- ============================================================================
-- 3. Summary by Activity Type (Тип активності)
-- ============================================================================
-- Global statistics by activity type

DROP MATERIALIZED VIEW IF EXISTS mv_activity_type_summary CASCADE;

CREATE MATERIALIZED VIEW mv_activity_type_summary AS
SELECT 
    at.id AS activity_type_id,
    at.name AS activity_type_name,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS total_hours,
    COUNT(DISTINCT t.id) AS themes_with_type,
    COUNT(DISTINCT sec.id) AS sections_with_type,
    ROUND(AVG(a.hours)::numeric, 2) AS avg_hours_per_activity
FROM activity_types at
LEFT JOIN activities a ON a.type_id = at.id
LEFT JOIN themes t ON a.theme_id = t.id
LEFT JOIN sections sec ON t.section_id = sec.id
GROUP BY at.id, at.name
ORDER BY at.id;

-- Index for fast lookups
CREATE UNIQUE INDEX idx_mv_activity_type_summary_id ON mv_activity_type_summary(activity_type_id);


-- ============================================================================
-- 4. Summary by Semester (Семестр)
-- ============================================================================
-- High-level semester statistics

DROP MATERIALIZED VIEW IF EXISTS mv_semester_summary CASCADE;

CREATE MATERIALIZED VIEW mv_semester_summary AS
SELECT 
    sem.id AS semester_id,
    sem.number AS semester_number,
    sem.weeks,
    sem.hours_per_week,
    COUNT(DISTINCT sec.id) AS section_count,
    COUNT(DISTINCT t.id) AS theme_count,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS total_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лекція' THEN a.hours ELSE 0 END), 0) AS lecture_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Практична' THEN a.hours ELSE 0 END), 0) AS practical_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лабораторна' THEN a.hours ELSE 0 END), 0) AS lab_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Самостійна' THEN a.hours ELSE 0 END), 0) AS self_study_hours
FROM semesters sem
LEFT JOIN sections sec ON sec.semester_id = sem.id
LEFT JOIN themes t ON t.section_id = sec.id
LEFT JOIN activities a ON a.theme_id = t.id
LEFT JOIN activity_types at ON a.type_id = at.id
GROUP BY sem.id, sem.number, sem.weeks, sem.hours_per_week
ORDER BY sem.number;

-- Index for fast lookups
CREATE UNIQUE INDEX idx_mv_semester_summary_id ON mv_semester_summary(semester_id);


-- ============================================================================
-- 5. Control Form Summary (Форми контролю)
-- ============================================================================
-- Statistics by control form

DROP MATERIALIZED VIEW IF EXISTS mv_control_form_summary CASCADE;

CREATE MATERIALIZED VIEW mv_control_form_summary AS
SELECT 
    cf.id AS control_form_id,
    cf.name AS control_form_name,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS total_hours,
    COUNT(DISTINCT t.id) AS themes_with_form
FROM control_forms cf
LEFT JOIN activities a ON a.control_form_id = cf.id
LEFT JOIN themes t ON a.theme_id = t.id
GROUP BY cf.id, cf.name
ORDER BY cf.id;

-- Index for fast lookups
CREATE UNIQUE INDEX idx_mv_control_form_summary_id ON mv_control_form_summary(control_form_id);


-- ============================================================================
-- Function to refresh all summary views
-- ============================================================================

CREATE OR REPLACE FUNCTION refresh_all_summaries()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_section_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_theme_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_activity_type_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_semester_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_control_form_summary;
    
    RAISE NOTICE 'All summary views refreshed successfully';
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Convenience views for common queries
-- ============================================================================

-- Grand total summary (single row)
DROP VIEW IF EXISTS v_grand_total CASCADE;

CREATE VIEW v_grand_total AS
SELECT 
    COUNT(DISTINCT sem.id) AS semester_count,
    COUNT(DISTINCT sec.id) AS section_count,
    COUNT(DISTINCT t.id) AS theme_count,
    COUNT(a.id) AS activity_count,
    COALESCE(SUM(a.hours), 0) AS total_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лекція' THEN a.hours ELSE 0 END), 0) AS lecture_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Практична' THEN a.hours ELSE 0 END), 0) AS practical_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Лабораторна' THEN a.hours ELSE 0 END), 0) AS lab_hours,
    COALESCE(SUM(CASE WHEN at.name = 'Самостійна' THEN a.hours ELSE 0 END), 0) AS self_study_hours
FROM semesters sem
LEFT JOIN sections sec ON sec.semester_id = sem.id
LEFT JOIN themes t ON t.section_id = sec.id
LEFT JOIN activities a ON a.theme_id = t.id
LEFT JOIN activity_types at ON a.type_id = at.id;

COMMIT;

-- ============================================================================
-- Usage examples:
-- ============================================================================
-- 
-- Get all section summaries:
--   SELECT * FROM mv_section_summary;
--
-- Get summary for specific semester:
--   SELECT * FROM mv_semester_summary WHERE semester_number = 5;
--
-- Get theme details for a section:
--   SELECT * FROM mv_theme_summary WHERE section_id = 4;
--
-- Get grand totals:
--   SELECT * FROM v_grand_total;
--
-- Refresh all summaries after ETL:
--   SELECT refresh_all_summaries();
--
-- ============================================================================
