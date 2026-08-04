CREATE TYPE gylo_job_state AS ENUM (
    'available', 'running', 'completed', 'discarded', 'cancelled'
);

CREATE TABLE gylo_job (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue            text           NOT NULL,
    task             text           NOT NULL,
    state            gylo_job_state NOT NULL DEFAULT 'available',
    priority         smallint       NOT NULL DEFAULT 0,
    payload          bytea          NOT NULL,

    attempt          smallint       NOT NULL DEFAULT 0,
    max_attempts     smallint       NOT NULL DEFAULT 20,
    errors           jsonb          NOT NULL DEFAULT '[]',

    scheduled_at     timestamptz    NOT NULL DEFAULT now(),
    created_at       timestamptz    NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finalized_at     timestamptz,

    locked_by        uuid,
    lease_expires_at timestamptz,

    metadata         jsonb          NOT NULL DEFAULT '{}',

    CONSTRAINT gylo_job_attempt_bounds CHECK (attempt <= max_attempts),
    CONSTRAINT gylo_job_lease_paired CHECK (
        (locked_by IS NULL) = (lease_expires_at IS NULL)
    )
);

CREATE INDEX gylo_job_fetch
    ON gylo_job (queue, priority, scheduled_at, id)
    WHERE state = 'available';

CREATE INDEX gylo_job_lease
    ON gylo_job (lease_expires_at)
    WHERE state = 'running';
