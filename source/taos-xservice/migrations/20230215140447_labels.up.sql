-- Add up migration script here
CREATE TABLE IF NOT EXISTS labels (
  `id` integer NOT NULL,
  `key` varchar NOT NULL,
  `value` varchar,
  CONSTRAINT fk_id FOREIGN KEY (`id`) REFERENCES tasks (`id`) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_labels_key ON labels (`key`);

CREATE UNIQUE INDEX IF NOT EXISTS unique_task_label_kv ON labels (`id`, `key`, `value`);

INSERT
  OR IGNORE INTO labels
  SELECT
    `id`,
    'to_cluster',
    `to_cluster`
  FROM
    tasks
  WHERE
    `to_cluster` IS NOT NULL
    AND `to_cluster` != '';

INSERT
  OR IGNORE INTO labels
  SELECT
    `id`,
    'from_cluster',
    `from_cluster`
  FROM
    tasks
  WHERE
    `from_cluster` IS NOT NULL
    AND `from_cluster` != '';

INSERT
  OR IGNORE INTO labels
  SELECT
    `id`,
    'stream_type',
    lower(`stream_type`)
  FROM
    tasks
  WHERE
    `stream_type` IS NOT NULL
    AND `stream_type` != '';

-- Unique task information with labels.
CREATE VIEW IF NOT EXISTS task_with_labels AS
SELECT
  tasks.id AS `id`,
  `from`,
  `to`,
  `oneshot_topic`,
  `jobs`,
  `compression_level`,
  `after_delete`,
  `created_at`,
  `last_modified_at`,
  `finished_at`,
  `deleted`,
  `status`,
  `status` == 'completed' AS `completed`,
  `status` == 'cancelled' AS `cancelled`,
  `reason`,
  `labels`,
  `stream_type`, -- deprecatd properties
  `from_cluster`,
  `to_cluster`,
  `force`
FROM
  tasks
  JOIN (
    SELECT
      id,
      json_group_array (iif (`value` IS NULL, `key`, `key` || '::' || `value`)) AS labels
    FROM
      labels
    GROUP BY
      id);

