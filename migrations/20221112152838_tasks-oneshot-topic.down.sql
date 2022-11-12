-- Add down migration script here
ALTER TABLE tasks DROP COLUMN `oneshot_topic`;
