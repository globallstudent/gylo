CREATE FUNCTION gylo_notify_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('gylo_available', ready.queue)
    FROM (
        SELECT DISTINCT queue FROM inserted WHERE scheduled_at <= now()
    ) AS ready;
    RETURN NULL;
END;
$$;

CREATE TRIGGER gylo_job_notify_insert
    AFTER INSERT ON gylo_job
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION gylo_notify_available();
