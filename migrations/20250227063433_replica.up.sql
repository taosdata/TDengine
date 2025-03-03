-- Add up migration script here
CREATE TABLE IF NOT EXISTS replicas(
  `id` varchar(8) PRIMARY KEY,
  `source` varchar(255) NOT NULL,
  `sink` varchar(255) NOT NULL,
  `jid` varchar(36),
  `topic_prefix` varchar(255),
  `group` varchar(255),
  `keep_topic_after_remove` boolean,
  `new_databases_checking_interval` int,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

