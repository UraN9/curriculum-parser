-- ============================================================================
-- CDC (Change Data Capture) via PostgreSQL Triggers
-- ============================================================================
-- Tracks all INSERT, UPDATE, DELETE operations on curriculum tables.
-- Changes are logged to `change_log` table for audit and processing.
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. Change Log Table
-- ============================================================================

DROP TABLE IF EXISTS change_log CASCADE;

CREATE TABLE change_log (
    id SERIAL PRIMARY KEY,
    
    -- When the change occurred
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- What table was changed
    table_name VARCHAR(50) NOT NULL,
    
    -- What operation: INSERT, UPDATE, DELETE
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    
    -- Record ID that was changed
    record_id INTEGER NOT NULL,
    
    -- Old values (for UPDATE and DELETE)
    old_data JSONB,
    
    -- New values (for INSERT and UPDATE)
    new_data JSONB,
    
    -- What fields changed (for UPDATE only)
    changed_fields TEXT[],
    
    -- Optional: who made the change (for future use with auth)
    changed_by VARCHAR(100),
    
    -- Flag for processing status
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP
);

-- Indexes for fast queries
CREATE INDEX idx_change_log_table ON change_log(table_name);
CREATE INDEX idx_change_log_operation ON change_log(operation);
CREATE INDEX idx_change_log_changed_at ON change_log(changed_at DESC);
CREATE INDEX idx_change_log_processed ON change_log(processed) WHERE NOT processed;


-- ============================================================================
-- 2. Generic Trigger Function
-- ============================================================================

CREATE OR REPLACE FUNCTION log_table_changes()
RETURNS TRIGGER AS $$
DECLARE
    old_json JSONB := NULL;
    new_json JSONB := NULL;
    changed_cols TEXT[] := ARRAY[]::TEXT[];
    col_name TEXT;
BEGIN
    -- Capture old data for UPDATE and DELETE
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        old_json := to_jsonb(OLD);
    END IF;
    
    -- Capture new data for INSERT and UPDATE
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        new_json := to_jsonb(NEW);
    END IF;
    
    -- For UPDATE, find which columns changed
    IF TG_OP = 'UPDATE' THEN
        FOR col_name IN 
            SELECT key FROM jsonb_object_keys(old_json) AS key
        LOOP
            IF old_json->col_name IS DISTINCT FROM new_json->col_name THEN
                changed_cols := array_append(changed_cols, col_name);
            END IF;
        END LOOP;
        
        -- Skip if nothing actually changed
        IF array_length(changed_cols, 1) IS NULL THEN
            RETURN NEW;
        END IF;
    END IF;
    
    -- Insert into change_log
    INSERT INTO change_log (
        table_name,
        operation,
        record_id,
        old_data,
        new_data,
        changed_fields
    ) VALUES (
        TG_TABLE_NAME,
        TG_OP,
        CASE 
            WHEN TG_OP = 'DELETE' THEN (old_json->>'id')::INTEGER
            ELSE (new_json->>'id')::INTEGER
        END,
        old_json,
        new_json,
        CASE WHEN array_length(changed_cols, 1) > 0 THEN changed_cols ELSE NULL END
    );
    
    -- Return appropriate value
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 3. Create Triggers on Curriculum Tables
-- ============================================================================

-- Sections table
DROP TRIGGER IF EXISTS trg_sections_cdc ON sections;
CREATE TRIGGER trg_sections_cdc
    AFTER INSERT OR UPDATE OR DELETE ON sections
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();

-- Themes table
DROP TRIGGER IF EXISTS trg_themes_cdc ON themes;
CREATE TRIGGER trg_themes_cdc
    AFTER INSERT OR UPDATE OR DELETE ON themes
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();

-- Activities table
DROP TRIGGER IF EXISTS trg_activities_cdc ON activities;
CREATE TRIGGER trg_activities_cdc
    AFTER INSERT OR UPDATE OR DELETE ON activities
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();

-- Semesters table
DROP TRIGGER IF EXISTS trg_semesters_cdc ON semesters;
CREATE TRIGGER trg_semesters_cdc
    AFTER INSERT OR UPDATE OR DELETE ON semesters
    FOR EACH ROW EXECUTE FUNCTION log_table_changes();


-- ============================================================================
-- 4. Auto-Refresh Summary Views on Changes
-- ============================================================================

CREATE OR REPLACE FUNCTION auto_refresh_summaries()
RETURNS TRIGGER AS $$
BEGIN
    -- Refresh materialized views after data changes
    -- Using non-concurrent refresh for simplicity (faster for small datasets)
    -- pg_advisory_xact_lock releases automatically at transaction end
    PERFORM pg_advisory_xact_lock(12345);  -- Prevent concurrent refreshes
    
    BEGIN
        REFRESH MATERIALIZED VIEW mv_section_summary;
        REFRESH MATERIALIZED VIEW mv_theme_summary;
        REFRESH MATERIALIZED VIEW mv_activity_type_summary;
        REFRESH MATERIALIZED VIEW mv_semester_summary;
        REFRESH MATERIALIZED VIEW mv_control_form_summary;
    EXCEPTION WHEN OTHERS THEN
        -- Log error but don't fail the transaction
        RAISE WARNING 'Failed to refresh summary views: %', SQLERRM;
    END;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to refresh summaries after changes to activities (main data table)
DROP TRIGGER IF EXISTS trg_refresh_summaries ON activities;
CREATE TRIGGER trg_refresh_summaries
    AFTER INSERT OR UPDATE OR DELETE ON activities
    FOR EACH STATEMENT EXECUTE FUNCTION auto_refresh_summaries();


-- ============================================================================
-- 5. Utility Functions
-- ============================================================================

-- Get recent changes
CREATE OR REPLACE FUNCTION get_recent_changes(
    p_limit INTEGER DEFAULT 50,
    p_table_name VARCHAR DEFAULT NULL,
    p_operation VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    id INTEGER,
    changed_at TIMESTAMP,
    table_name VARCHAR,
    operation VARCHAR,
    record_id INTEGER,
    old_data JSONB,
    new_data JSONB,
    changed_fields TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cl.id,
        cl.changed_at,
        cl.table_name,
        cl.operation,
        cl.record_id,
        cl.old_data,
        cl.new_data,
        cl.changed_fields
    FROM change_log cl
    WHERE (p_table_name IS NULL OR cl.table_name = p_table_name)
      AND (p_operation IS NULL OR cl.operation = p_operation)
    ORDER BY cl.changed_at DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;


-- Mark changes as processed
CREATE OR REPLACE FUNCTION mark_changes_processed(p_ids INTEGER[])
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE change_log
    SET processed = TRUE,
        processed_at = NOW()
    WHERE id = ANY(p_ids)
      AND NOT processed;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;


-- Get unprocessed changes count
CREATE OR REPLACE FUNCTION get_unprocessed_count()
RETURNS TABLE (
    table_name VARCHAR,
    operation VARCHAR,
    count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cl.table_name,
        cl.operation,
        COUNT(*)::BIGINT
    FROM change_log cl
    WHERE NOT cl.processed
    GROUP BY cl.table_name, cl.operation
    ORDER BY cl.table_name, cl.operation;
END;
$$ LANGUAGE plpgsql;


-- Cleanup old processed changes (retention policy)
CREATE OR REPLACE FUNCTION cleanup_old_changes(p_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM change_log
    WHERE processed = TRUE
      AND processed_at < NOW() - (p_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;


COMMIT;

-- ============================================================================
-- Usage Examples:
-- ============================================================================
--
-- View recent changes:
--   SELECT * FROM get_recent_changes(10);
--
-- View changes for specific table:
--   SELECT * FROM get_recent_changes(10, 'activities');
--
-- View only INSERT operations:
--   SELECT * FROM get_recent_changes(10, NULL, 'INSERT');
--
-- Get unprocessed changes count:
--   SELECT * FROM get_unprocessed_count();
--
-- Mark changes as processed:
--   SELECT mark_changes_processed(ARRAY[1, 2, 3]);
--
-- Cleanup old changes (older than 30 days):
--   SELECT cleanup_old_changes(30);
--
-- Direct query to change_log:
--   SELECT * FROM change_log ORDER BY changed_at DESC LIMIT 20;
--
-- ============================================================================
