-- Add down migration script here
ALTER TABLE agent_activities
  DROP COLUMN `level`;

ALTER TABLE task_activities
  DROP COLUMN `level`;

ALTER TABLE task_activities
  DROP COLUMN `status`;

ALTER TABLE agents
  ADD COLUMN `status` varchar;
