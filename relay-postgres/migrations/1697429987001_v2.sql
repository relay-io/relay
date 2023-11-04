CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Alter table constraints
ALTER TABLE jobs ALTER COLUMN max_retries DROP NOT NULL;
ALTER TABLE jobs ALTER COLUMN retries_remaining DROP NOT NULL;

-- Reset any data needed before changing any column types
UPDATE jobs SET max_retries = 32767 WHERE max_retries > 32767;
UPDATE jobs SET retries_remaining = 32767 WHERE retries_remaining > 32767;
UPDATE jobs SET max_retries = NULL WHERE max_retries < 0;

-- Alter table columns
ALTER TABLE jobs ALTER COLUMN max_retries TYPE smallint;
ALTER TABLE jobs ALTER COLUMN retries_remaining TYPE smallint;

-- Add new columns
ALTER TABLE jobs ADD COLUMN run_id uuid DEFAULT NULL;

-- Post reset any data.
UPDATE jobs SET in_flight = FALSE WHERE in_flight;