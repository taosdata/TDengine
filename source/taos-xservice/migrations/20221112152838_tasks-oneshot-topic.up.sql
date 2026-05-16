-- Add up migration script here
ALTER TABLE tasks ADD COLUMN `oneshot_topic` VARCHAR(192) DEFAULT NULL;
