SET search_path TO activities, public;

ALTER TABLE activities
    ADD COLUMN original_digest text NOT NULL DEFAULT '',
    ADD COLUMN data_digest text NOT NULL DEFAULT '',
    DROP COLUMN completed_data,
    DROP COLUMN completed_original;
