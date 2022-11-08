-- Add migration script here
CREATE TABLE IF NOT EXISTS tasks (
  `id` integer PRIMARY KEY AUTOINCREMENT,
  -- task properties
  `stream_type` varchar NOT NULL,
  `from` varchar NOT NULL,
  `from_cluster` varchar,
  `to` varchar NOT NULL,
  `to_cluster` varchar,
  `jobs` int NOT NULL DEFAULT 0,
  `compression_level` int DEFAULT NULL,
  `force` boolean DEFAULT NULL,
  -- @begin task states
  `created_at` datetime,
  `finished_at` datetime,
  `last_modified_at` datetime,
  `status` varchar NOT NULL,
  `reason` varchar,
  `deleted` boolean NOT NULL DEFAULT FALSE
  -- @end
);


CREATE INDEX IF NOT EXISTS idx_created_at ON tasks (`created_at`);
CREATE INDEX IF NOT EXISTS idx_status ON tasks (`status`);
CREATE INDEX IF NOT EXISTS idx_deleted ON tasks (`deleted`);
CREATE INDEX IF NOT EXISTS idx_from_cluster ON tasks (`from_cluster`);
CREATE INDEX IF NOT EXISTS idx_tp_cluster ON tasks (`to_cluster`);
