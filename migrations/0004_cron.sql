CREATE TABLE gylo_cron (
    name        text        PRIMARY KEY,
    queue       text        NOT NULL,
    task        text        NOT NULL,
    payload     bytea       NOT NULL,
    expression  text        NOT NULL,
    timezone    text        NOT NULL DEFAULT 'UTC',
    paused      boolean     NOT NULL DEFAULT false,
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX gylo_cron_due ON gylo_cron (next_run_at) WHERE NOT paused;
