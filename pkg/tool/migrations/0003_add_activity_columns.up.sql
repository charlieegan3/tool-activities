SET search_path TO activities, public;

ALTER TABLE activities
    ADD COLUMN completed_data BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN completed_original BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN type VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN gear_id VARCHAR(255) NOT NULL DEFAULT '',
    DROP COLUMN completed;


