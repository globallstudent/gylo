-- Answers "is any keyed job waiting" as an index probe rather than a scan.
--
-- Admission for a key has to be serialised, and serialising every fetch to
-- protect it costs about four times the throughput. This index lets the common
-- fetch establish in one probe that there is nothing keyed to serialise for,
-- and take the fast path.
CREATE INDEX gylo_job_keyed_waiting
    ON gylo_job (queue, scheduled_at)
    WHERE concurrency_key IS NOT NULL AND state = 'available';
