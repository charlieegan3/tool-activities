SET search_path TO activities, public;

CREATE TYPE activity_source AS ENUM ('export', 'polling');

CREATE TABLE IF NOT EXISTS activities(
    id TEXT NOT NULL PRIMARY KEY,

    source activity_source NOT NULL,

    completed BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
