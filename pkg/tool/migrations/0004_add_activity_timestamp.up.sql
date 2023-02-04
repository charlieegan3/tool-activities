SET search_path TO activities, public;

ALTER TABLE activities
    ADD COLUMN timestamp timestamp with time zone NOT NULL DEFAULT 'epoch'::timestamp;
