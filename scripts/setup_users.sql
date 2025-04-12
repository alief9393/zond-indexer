-- scripts/setup_user.sql

-- Create the zond_app user if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles WHERE rolname = 'zond_app'
    ) THEN
        CREATE ROLE zond_app WITH LOGIN PASSWORD 'P@ssw0rd';
    END IF;
END $$;

-- Grant CONNECT privilege on the database
GRANT CONNECT ON DATABASE zond_indexer_db TO zond_app;

-- Grant USAGE on the public schema
GRANT USAGE ON SCHEMA public TO zond_app;

-- Grant specific privileges on existing tables
-- The indexer needs SELECT, INSERT, UPDATE on most tables
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO zond_app;

-- For tables where the indexer might need to delete (e.g., during testing or cleanup)
-- GRANT DELETE ON ALL TABLES IN SCHEMA public TO zond_app;

-- Grant privileges on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT, INSERT, UPDATE ON TABLES TO zond_app;

-- Grant USAGE and SELECT on sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO zond_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT USAGE, SELECT ON SEQUENCES TO zond_app;

-- Optional: If the app needs to create/drop tables (e.g., during migrations with --drop-db)
-- GRANT CREATE ON SCHEMA public TO zond_app;