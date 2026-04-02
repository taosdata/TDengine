-- Add down migration script here
DROP VIEW IF EXISTS task_with_labels;

CREATE VIEW IF NOT EXISTS task_with_labels AS
SELECT
  tasks.*,
  `status` == 'completed' AS `completed`,
  `status` == 'cancelled' AS `cancelled`,
  `labels`
FROM
  tasks
  LEFT OUTER JOIN (
  SELECT
    id,
    json_group_array (iif (`value` IS NULL, `key`, `key` || '::' || `value`)) AS labels
  FROM
    labels
  GROUP BY
    id) label ON (tasks.id = label.id);

ALTER TABLE tasks
  DROP COLUMN `trigger`;

ALTER TABLE tasks
  DROP COLUMN `name`;

DROP TABLE IF EXISTS task_activities;

