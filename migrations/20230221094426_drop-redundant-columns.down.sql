-- Add down migration script here
DROP VIEW IF EXISTS task_with_labels;

ALTER TABLE tasks
  ADD COLUMN `stream_type` varchar;

ALTER TABLE tasks
  ADD COLUMN `from_cluster` varchar;

ALTER TABLE tasks
  ADD COLUMN `to_cluster` varchar;

ALTER TABLE tasks
  ADD COLUMN `force` boolean;

CREATE INDEX IF NOT EXISTS idx_from_cluster ON tasks (`from_cluster`);
CREATE INDEX IF NOT EXISTS idx_tp_cluster ON tasks (`to_cluster`);

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

