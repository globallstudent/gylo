CREATE TABLE gylo_workflow (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE gylo_job
    ADD COLUMN workflow_id  bigint REFERENCES gylo_workflow (id) ON DELETE CASCADE,
    ADD COLUMN pending_deps int NOT NULL DEFAULT 0;

ALTER TABLE gylo_job ADD CONSTRAINT gylo_job_pending_deps_positive
    CHECK (pending_deps >= 0);

CREATE TABLE gylo_edge (
    workflow_id bigint NOT NULL REFERENCES gylo_workflow (id) ON DELETE CASCADE,
    parent      bigint NOT NULL REFERENCES gylo_job (id) ON DELETE CASCADE,
    child       bigint NOT NULL REFERENCES gylo_job (id) ON DELETE CASCADE,
    PRIMARY KEY (parent, child)
);

CREATE INDEX gylo_edge_parent ON gylo_edge (parent);
