-- Add up migration script here
ALTER TABLE tasks
  ADD COLUMN `name` VARCHAR(192) DEFAULT NULL;

ALTER TABLE tasks
  ADD COLUMN `trigger` VARCHAR DEFAULT NULL;

DROP VIEW IF EXISTS task_with_labels;

-- Use tasks.* for forward compatibility
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

-- Task related activities.
CREATE TABLE IF NOT EXISTS task_activities (
  `id` integer NOT NULL,
  `at` datetime NOT NULL,
  `activity` varchar NOT NULL,
  `context` varchar DEFAULT NULL,
  CONSTRAINT fk_activities_id FOREIGN KEY (`id`) REFERENCES tasks (`id`) ON DELETE CASCADE
);

-- Activities task id index.
CREATE INDEX IF NOT EXISTS idx_activities_id ON task_activities (`id`);

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  id,
  created_at,
  'Created'
FROM
  tasks;

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  `id`,
  `finished_at`,
  `status`
FROM
  tasks
WHERE
  tasks.status = 'Completed';

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  id,
  finished_at,
  `status`
FROM
  tasks
WHERE
  tasks.status = 'Cancelled';

INSERT INTO task_activities (`id`, `at`, `activity`, `context`)
SELECT
  id,
  finished_at,
  `status`,
  reason
FROM
  tasks
WHERE
  tasks.status = 'Failed';

INSERT INTO task_activities (`id`, `at`, `activity`, `context`)
SELECT
  id,
  finished_at,
  `status`,
  reason
FROM
  tasks
WHERE
  tasks.status = 'Interrupted';

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  id,
  last_modified_at,
  `status`
FROM
  tasks
WHERE
  tasks.status = 'Stopped';

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  id,
  last_modified_at,
  `status`
FROM
  tasks
WHERE
  tasks.status = 'Started';

INSERT INTO task_activities (`id`, `at`, `activity`)
SELECT
  id,
  last_modified_at,
  `status`
FROM
  tasks
WHERE
  tasks.status = 'Deleted';

