-- Add up migration script here

DROP VIEW task_with_labels;

DROP INDEX IF EXISTS idx_from_cluster;

DROP INDEX IF EXISTS idx_tp_cluster;

ALTER TABLE tasks
  DROP COLUMN `stream_type`;

ALTER TABLE tasks
  DROP COLUMN `from_cluster`;

ALTER TABLE tasks
  DROP COLUMN `to_cluster`;

ALTER TABLE tasks
  DROP COLUMN `force`;

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
  `labels`
FROM
  tasks
  INNER JOIN (
    SELECT
      id,
      json_group_array (iif (`value` IS NULL, `key`, `key` || '::' || `value`)) AS labels
    FROM
      labels
    GROUP BY
      id) label ON (tasks.id = label.id);

