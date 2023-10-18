-- Add down migration script here
DROP INDEX IF EXISTS idx_tasks_name;
DROP INDEX IF EXISTS idx_tasks_agent;
DROP INDEX IF EXISTS idx_tasks_created;
