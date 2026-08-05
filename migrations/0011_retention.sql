-- Finds retention victims in finalisation order rather than by scanning the
-- table they are supposed to keep small.
CREATE INDEX gylo_job_retention
    ON gylo_job (finalized_at)
    WHERE state IN ('completed', 'discarded', 'cancelled');

-- Serves both the is-any-member-unfinished check that keeps retention off a
-- live workflow, and the cascade when a workflow row itself is deleted.
CREATE INDEX gylo_job_workflow
    ON gylo_job (workflow_id)
    WHERE workflow_id IS NOT NULL;
