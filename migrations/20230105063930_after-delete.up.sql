-- Add up migration script here

ALTER TABLE tasks ADD COLUMN `after_delete` VARCHAR(192) DEFAULT NULL;
