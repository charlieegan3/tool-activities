SET search_path TO activities, public;

ALTER TABLE activities ADD COLUMN original_format text NOT NULL DEFAULT '';
