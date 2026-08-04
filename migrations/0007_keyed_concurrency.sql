ALTER TABLE gylo_job
    ADD COLUMN concurrency_key text,
    ADD COLUMN max_concurrency int;

ALTER TABLE gylo_job ADD CONSTRAINT gylo_job_concurrency_paired CHECK (
    (concurrency_key IS NULL) = (max_concurrency IS NULL)
);

ALTER TABLE gylo_job ADD CONSTRAINT gylo_job_concurrency_positive CHECK (
    max_concurrency IS NULL OR max_concurrency > 0
);

CREATE INDEX gylo_job_concurrency
    ON gylo_job (concurrency_key)
    WHERE concurrency_key IS NOT NULL AND state = 'running';
