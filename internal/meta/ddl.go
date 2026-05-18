package meta

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const SchemaDDL = `
CREATE SCHEMA IF NOT EXISTS _chuck;

CREATE SEQUENCE IF NOT EXISTS _chuck.overlay_seq;

CREATE TABLE IF NOT EXISTS _chuck._chuck_branches (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT UNIQUE NOT NULL,
    parent_id       UUID REFERENCES _chuck._chuck_branches(id),
    status          TEXT NOT NULL DEFAULT 'active',  -- active | merging | merged | dropped
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_seconds     INTEGER,
    merge_status    TEXT
);

CREATE TABLE IF NOT EXISTS _chuck._chuck_branch_tables (
    branch_id       UUID REFERENCES _chuck._chuck_branches(id) ON DELETE CASCADE,
    table_name      TEXT NOT NULL,
    mode            TEXT NOT NULL,  -- overlay | shared
    PRIMARY KEY (branch_id, table_name)
);

CREATE TABLE IF NOT EXISTS _chuck._chuck_checkpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    branch_id       UUID REFERENCES _chuck._chuck_branches(id) ON DELETE CASCADE,
    label           TEXT,
    overlay_seq     BIGINT NOT NULL,  -- watermark into _chuck_overlay
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS _chuck._chuck_schema_ops (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    branch_id       UUID REFERENCES _chuck._chuck_branches(id) ON DELETE CASCADE,
    operation_type  TEXT NOT NULL,  -- ADD_COLUMN | DROP_COLUMN | ADD_INDEX | etc
    table_name      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS _chuck._chuck_overlay (
    branch_id     UUID        NOT NULL,
    table_name    TEXT        NOT NULL,
    row_id        BIGINT      NOT NULL,
    shard_key     TEXT        NOT NULL DEFAULT 'default', -- For future co-sharded colocation with base tables
    operation     TEXT        NOT NULL,  -- INSERT | UPDATE | DELETE
    modified_cols JSONB,                 -- sparse: only changed columns
    before_values JSONB,                 -- for conflict detection on merge
    after_values  JSONB,                 -- complete row state for overlay reads
    seq           BIGINT      NOT NULL DEFAULT nextval('_chuck.overlay_seq'),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (branch_id, table_name, row_id, shard_key)
);

CREATE INDEX IF NOT EXISTS idx_chuck_overlay_lookup ON _chuck._chuck_overlay (branch_id, table_name, row_id, shard_key);
CREATE INDEX IF NOT EXISTS idx_chuck_overlay_seq ON _chuck._chuck_overlay (branch_id, seq);
`

// Bootstrap idempotently initializes the _chuck schema, sequences, tables, and indexes.
func Bootstrap(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, SchemaDDL)
	if err != nil {
		return fmt.Errorf("failed to bootstrap _chuck metadata schema: %w", err)
	}
	return nil
}
