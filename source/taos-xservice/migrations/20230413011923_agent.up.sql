
-- Global secret
CREATE TABLE IF NOT EXISTS `secret` (`secret` VARCHAR(64));

-- Agent for taosX, an agent will directly bind to a TDengine data source.
CREATE TABLE IF NOT EXISTS `agents`(
  `id` integer PRIMARY KEY AUTOINCREMENT, -- Agent id.
  `name` varchar NOT NULL,
  `cluster_id` varchar NOT NULL,
  `dsn` varchar NOT NULL, -- TDengine data source connection string in DSN format.
  `user_id` varchar NOT NULL, -- Agent created by user
  `expire_date` date, -- expire date
  `connectors` varchar, -- allowed connectors
  `created_at` datetime, -- Created at datetime
  `last_modified_at` datetime,
  `status` varchar
);

CREATE TABLE IF NOT EXISTS `agent_activities` (
  `id` integer REFERENCES `agents` (`id`),
  `at` datetime NOT NULL,
  `activity` varchar NOT NULL,
  `status` varchar NOT NULL,
  `context` varchar
);

CREATE INDEX IF NOT EXISTS idx_agents_of_cluster_user ON `agents`(`cluster_id`, `user_id`);

-- Data Sources.
CREATE TABLE IF NOT EXISTS `datasource`(
  `id` integer PRIMARY KEY AUTOINCREMENT, -- data source id
  `dsn` varchar NOT NULL, -- Data Source Name
  `agent_id` integer,
  CONSTRAINT fk_ds_agent_id FOREIGN KEY (`id`) REFERENCES `agents`(`id`) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_datasource_dsn ON `datasource`(`dsn`);

CREATE INDEX IF NOT EXISTS idx_datasource_of_agent ON `datasource`(`agent_id`);

-- Alter tasks
ALTER TABLE tasks
  ADD COLUMN `via` integer;

