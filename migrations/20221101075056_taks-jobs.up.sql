-- Add up migration script here
ALTER TABLE tasks ADD `jobs` INT NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD `compression_level` INT DEFAULT NULL;
ALTER TABLE tasks ADD `force` BOOLEAN DEFAULT NULL;
