CREATE SCHEMA IF NOT EXISTS chuck_meta;

CREATE TABLE IF NOT EXISTS chuck_meta.branches (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    schema_name      TEXT NOT NULL UNIQUE,

    parent_branch_id BIGINT REFERENCES chuck_meta.branches(id),
    base_commit_id   UUID,

    sequence_offsets JSONB NOT NULL DEFAULT '{}',

    status           TEXT NOT NULL DEFAULT 'active',

    locked_by        TEXT,
    locked_at        TIMESTAMPTZ,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    merged_at        TIMESTAMPTZ,
    dropped_at       TIMESTAMPTZ,
    last_conflict    TIMESTAMPZ
);

CREATE TABLE IF NOT EXISTS chuck_meta.tracked_tables (
    id             BIGSERIAL PRIMARY KEY,
    table_oid      OID,

    table_schema   TEXT NOT NULL DEFAULT 'public',
    table_name     TEXT NOT NULL,

    primary_keys   TEXT[] NOT NULL,

    UNIQUE (table_schema, table_name)
);

CREATE TABLE IF NOT EXISTS chuck_meta.branch_tables (
    id                  BIGSERIAL PRIMARY KEY,
    branch_id           BIGINT NOT NULL REFERENCES chuck_meta.branches(id),
    table_id            BIGINT NOT NULL REFERENCES chuck_meta.tracked_tables(id),
    delta_table         TEXT NOT NULL,
    view_name           TEXT NOT NULL,
    view_tier           TEXT NOT NULL DEFAULT 'passthrough',
    suspended_fks       JSONB NOT NULL DEFAULT '[]',
    cascade_chains      JSONB NOT NULL DEFAULT '[]',
    view_dependencies   JSONB NOT NULL DEFAULT '[]',
    replicated_triggers JSONB NOT NULL DEFAULT '[]',
    is_dirty            BOOLEAN NOT NULL DEFAULT false,
    last_modified_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS chuck_meta.merge_history (
    id             BIGSERIAL PRIMARY KEY,
    branch_id      BIGINT NOT NULL REFERENCES chuck_meta.branches(id),
    merged_by      TEXT,
    merged_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    fk_violations  JSONB,
    pk_remaps      JSONB,
    success        BOOLEAN NOT NULL,
    error_message  TEXT,
    duration_ms    BIGINT,
    conflict_summary JSONB
);

CREATE TABLE IF NOT EXISTS chuck_meta.commits (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    branch_id     BIGINT NOT NULL REFERENCES chuck_meta.branches(id),
    parent_ids    UUID[] NOT NULL DEFAULT '{}',
    message       TEXT,
    author        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delta_summary JSONB,
    row_inserts   BIGINT DEFAULT 0,
    row_updates   BIGINT DEFAULT 0,
    row_deletes   BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chuck_meta.active_branch (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CONSTRAINT active_branch_singleton CHECK (singleton),
    branch_id BIGINT NOT NULL REFERENCES chuck_meta.branches(id),
    switched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
