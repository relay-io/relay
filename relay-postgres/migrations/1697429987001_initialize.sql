CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS jobs (
    id                varchar NOT NULL,
    queue             varchar NOT NULL,
    timeout           interval NOT NULL,
    max_retries       integer DEFAULT NULL, --  TODO: change to SMALLINT
    retries_remaining integer DEFAULT NULL, --  TODO: change to SMALLINT
    data              jsonb NOT NULL,
    state             jsonb DEFAULT NULL,
    in_flight         boolean DEFAULT FALSE,
    run_id            uuid DEFAULT NULL,
    run_at            timestamp without time zone NOT NULL,
    expires_at        timestamp without time zone,
    updated_at        timestamp without time zone NOT NULL,
    created_at        timestamp without time zone NOT NULL,
    PRIMARY KEY (queue, id)
);
CREATE INDEX IF NOT EXISTS idx_queue_in_flight_created_at ON jobs (queue, in_flight, run_at);
CREATE INDEX IF NOT EXISTS idx_queue_expires_remain ON jobs (in_flight, expires_at, retries_remaining NULLS LAST);

CREATE TABLE IF NOT EXISTS internal_state (
    id varchar NOT NULL,
    last_run timestamp without time zone NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO internal_state (id, last_run) VALUES ('reap', NOW()) ON CONFLICT DO NOTHING;