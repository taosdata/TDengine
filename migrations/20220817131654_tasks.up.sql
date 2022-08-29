-- Add migration script here
CREATE TABLE IF NOT EXISTS tasks (
  `id` integer PRIMARY KEY AUTOINCREMENT,
  `from` varchar NOT NULL,
  `to` varchar NOT NULL,
  `stream_type` varchar NOT NULL,
  `created_at` datetime,
  `finished_at` datetime,
  `last_modified_at` datetime,
  `status` varchar NOT NULL,
  `reason` varchar,
  `deleted` boolean NOT NULL DEFAULT FALSE
);

