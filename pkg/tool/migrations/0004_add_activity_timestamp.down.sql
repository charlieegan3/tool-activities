SET search_path TO activities, public;

ALTER TABLE activities
    DROP COLUMN timestamp;
