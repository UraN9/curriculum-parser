-- ============================================================================
-- Database-Level Authorization with Row-Level Security (RLS)
-- ============================================================================
-- This script creates database roles and RLS policies for two-level authorization:
-- 1. app_admin - Full access to all data
-- 2. app_viewer - Limited access (only own data for certain tables)
-- ============================================================================

BEGIN;

-- ============================================================================
-- Create Application Roles
-- ============================================================================

-- Drop roles if they exist (for idempotent script)
DO $$
BEGIN
    -- Revoke privileges before dropping
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_admin') THEN
        EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM app_admin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_viewer') THEN
        EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM app_viewer';
    END IF;
END
$$;

-- Create app_admin role with full privileges
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_admin') THEN
        CREATE ROLE app_admin;
    END IF;
END
$$;

-- Create app_viewer role with limited privileges
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_viewer') THEN
        CREATE ROLE app_viewer;
    END IF;
END
$$;

-- ============================================================================
-- Grant Privileges
-- ============================================================================

-- app_admin: Full access to all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO app_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_admin;

-- app_viewer: Read access to most tables, limited write access
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_viewer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_viewer;

-- Allow viewers to update their own profile (lecturers and students tables)
GRANT UPDATE (full_name, email, password_hash) ON lecturers TO app_viewer;
GRANT UPDATE (full_name, email, password_hash) ON students TO app_viewer;

-- ============================================================================
-- Enable Row-Level Security
-- ============================================================================

-- Enable RLS on tables that need user-based access control
ALTER TABLE lecturers ENABLE ROW LEVEL SECURITY;
ALTER TABLE students ENABLE ROW LEVEL SECURITY;
ALTER TABLE disciplines ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- RLS Policies for Lecturers Table
-- ============================================================================

-- Drop existing policies if they exist
DROP POLICY IF EXISTS lecturers_admin_all ON lecturers;
DROP POLICY IF EXISTS lecturers_viewer_select ON lecturers;
DROP POLICY IF EXISTS lecturers_viewer_update ON lecturers;

-- Admin can do everything
CREATE POLICY lecturers_admin_all ON lecturers
    FOR ALL
    USING (current_setting('app.current_role', true) = 'admin')
    WITH CHECK (current_setting('app.current_role', true) = 'admin');

-- Viewer can only see their own record
CREATE POLICY lecturers_viewer_select ON lecturers
    FOR SELECT
    USING (
        current_setting('app.current_role', true) = 'admin'
        OR id::text = current_setting('app.current_user_id', true)
    );

-- Viewer can only update their own record
CREATE POLICY lecturers_viewer_update ON lecturers
    FOR UPDATE
    USING (id::text = current_setting('app.current_user_id', true))
    WITH CHECK (id::text = current_setting('app.current_user_id', true));

-- ============================================================================
-- RLS Policies for Students Table
-- ============================================================================

-- Drop existing policies if they exist
DROP POLICY IF EXISTS students_admin_all ON students;
DROP POLICY IF EXISTS students_viewer_select ON students;
DROP POLICY IF EXISTS students_viewer_update ON students;

-- Admin can do everything
CREATE POLICY students_admin_all ON students
    FOR ALL
    USING (current_setting('app.current_role', true) = 'admin')
    WITH CHECK (current_setting('app.current_role', true) = 'admin');

-- Viewer can only see their own record
CREATE POLICY students_viewer_select ON students
    FOR SELECT
    USING (
        current_setting('app.current_role', true) = 'admin'
        OR id::text = current_setting('app.current_user_id', true)
    );

-- Viewer can only update their own record
CREATE POLICY students_viewer_update ON students
    FOR UPDATE
    USING (id::text = current_setting('app.current_user_id', true))
    WITH CHECK (id::text = current_setting('app.current_user_id', true));

-- ============================================================================
-- RLS Policies for Disciplines Table
-- ============================================================================

-- Drop existing policies if they exist
DROP POLICY IF EXISTS disciplines_admin_all ON disciplines;
DROP POLICY IF EXISTS disciplines_viewer_select ON disciplines;

-- Admin can do everything
CREATE POLICY disciplines_admin_all ON disciplines
    FOR ALL
    USING (current_setting('app.current_role', true) = 'admin')
    WITH CHECK (current_setting('app.current_role', true) = 'admin');

-- Viewers can see all disciplines (public data) but can't modify
CREATE POLICY disciplines_viewer_select ON disciplines
    FOR SELECT
    USING (true);

-- ============================================================================
-- Helper Function for Session Context
-- ============================================================================

-- Function to set current user context for RLS
CREATE OR REPLACE FUNCTION set_current_user_context(
    p_user_id INTEGER,
    p_role TEXT
) RETURNS VOID AS $$
BEGIN
    PERFORM set_config('app.current_user_id', p_user_id::text, false);
    PERFORM set_config('app.current_role', p_role, false);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION set_current_user_context(INTEGER, TEXT) TO app_admin, app_viewer;

-- ============================================================================
-- Force RLS for table owners (important for security)
-- ============================================================================

-- By default, table owners bypass RLS. This ensures RLS is applied to everyone.
ALTER TABLE lecturers FORCE ROW LEVEL SECURITY;
ALTER TABLE students FORCE ROW LEVEL SECURITY;
ALTER TABLE disciplines FORCE ROW LEVEL SECURITY;

COMMIT;

-- ============================================================================
-- Usage Notes:
-- ============================================================================
-- 
-- Before executing queries, set the user context:
--   SELECT set_current_user_context(1, 'admin');
--   -- or --
--   SELECT set_current_user_context(5, 'viewer');
--
-- Then execute your queries - RLS will automatically filter results.
--
-- Example:
--   SELECT set_current_user_context(1, 'viewer');
--   SELECT * FROM lecturers;  -- Will only return lecturer with id=1
-- ============================================================================
