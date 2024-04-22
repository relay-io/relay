CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS jobs (
    id           varchar(1024) NOT NULL,
    queue        varchar(1024) NOT NULL,
    timeout      interval NOT NULL,
    max_retries  smallint,
    retries_remaining smallint,
    data         jsonb NOT NULL,
    state        jsonb DEFAULT NULL,
    in_flight    boolean DEFAULT FALSE,
    expires_at   timestamp without time zone,
    updated_at   timestamp without time zone NOT NULL,
    created_at   timestamp without time zone NOT NULL,
    run_at       timestamp without time zone DEFAULT NULL,
    run_id       uuid DEFAULT NULL,
    parent_id    varchar(1024) DEFAULT NULL,
    parent_queue varchar(1024) DEFAULT NULL,
    children_remaining smallint DEFAULT NULL,
    ultimate_parent_id    varchar(1024) DEFAULT NULL,
    ultimate_parent_queue varchar(1024) DEFAULT NULL,
    workflow_results jsonb DEFAULT NULL,
    PRIMARY KEY (queue, id)
);
CREATE INDEX IF NOT EXISTS idx_queue_in_flight_created_at ON jobs (queue, in_flight, run_at);
CREATE INDEX IF NOT EXISTS idx_queue_expires_remain ON jobs (in_flight, expires_at, retries_remaining);
CREATE INDEX IF NOT EXISTS idx_upid_upqueue ON jobs (ultimate_parent_id, ultimate_parent_queue);

CREATE TABLE IF NOT EXISTS internal_state (
  id varchar NOT NULL,
  last_run timestamp without time zone NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO internal_state (id, last_run) VALUES ('reap', NOW()) ON CONFLICT DO NOTHING;