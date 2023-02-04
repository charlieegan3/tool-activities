SET search_path TO activities, public;

ALTER TABLE activities
    DROP COLUMN completed_data,
    DROP COLUMN completed_original,
    DROP COLUMN type,
    DROP COLUMN gear_id,
    ADD COLUMN completed BOOLEAN NOT NULL DEFAULT FALSE;
