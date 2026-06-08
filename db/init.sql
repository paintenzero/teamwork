-- Phase 0 provisions these tables; Phase 3 starts writing to them.
-- The live observability stream lives in Redis; Postgres holds the queryable
-- spine, and full raw traces get archived to S3 as JSONL with a pointer here.

CREATE TABLE IF NOT EXISTS agents (
  id            TEXT PRIMARY KEY,
  capabilities  TEXT[] NOT NULL DEFAULT '{}',
  metadata      JSONB NOT NULL DEFAULT '{}',
  last_seen     TIMESTAMPTZ,
  first_seen    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
  id                 UUID PRIMARY KEY,           -- uuidv7
  agent_id           TEXT REFERENCES agents(id),
  parent_session_id  UUID REFERENCES sessions(id), -- delegation tree
  status             TEXT NOT NULL DEFAULT 'open',
  summary            TEXT,
  trace_uri          TEXT,                       -- S3 JSONL archive of full trace
  started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at           TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS sessions_parent_idx ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS sessions_agent_idx  ON sessions(agent_id);

-- Clean request/reply audit (no thinking/tool noise — that's in the trace).
CREATE TABLE IF NOT EXISTS messages (
  id              UUID PRIMARY KEY,
  session_id      UUID REFERENCES sessions(id),
  correlation_id  UUID,
  direction       TEXT NOT NULL,                 -- 'request' | 'response'
  source          TEXT NOT NULL,
  target          TEXT,
  payload         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_session_idx ON messages(session_id);
CREATE INDEX IF NOT EXISTS messages_corr_idx    ON messages(correlation_id);

CREATE TABLE IF NOT EXISTS artifacts (
  id            UUID PRIMARY KEY,
  session_id    UUID REFERENCES sessions(id),
  s3_uri        TEXT NOT NULL,
  content_type  TEXT,
  name          TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS artifacts_session_idx ON artifacts(session_id);
