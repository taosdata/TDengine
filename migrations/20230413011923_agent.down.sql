-- Add down migration script here
ALTER TABLE tasks
  DROP COLUMN `via`;

DROP TABLE IF EXISTS `datasource`;

DROP TABLE IF EXISTS `agent_activities`;

DROP TABLE IF EXISTS `agents`;

DROP TABLE IF EXISTS `secret`;

