SET search_path TO activities, public;

ALTER TABLE activities
    DROP COLUMN data_digest,
    DROP COLUMN original_digest,
    ADD COLUMN completed_data BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN completed_original BOOLEAN NOT NULL DEFAULT FALSE;
