ALTER TABLE gylo_job ADD COLUMN durable boolean NOT NULL DEFAULT false;

CREATE TABLE gylo_step (
    job_id     bigint      NOT NULL REFERENCES gylo_job (id) ON DELETE CASCADE,
    name       text        NOT NULL,
    result     bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, name)
);
