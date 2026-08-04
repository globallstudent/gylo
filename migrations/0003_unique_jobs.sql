ALTER TABLE gylo_job ADD COLUMN unique_key bytea;

CREATE UNIQUE INDEX gylo_job_unique
    ON gylo_job (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('available', 'running');
