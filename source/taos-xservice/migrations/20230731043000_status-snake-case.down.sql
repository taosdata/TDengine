-- Add down migration script here
CREATE TABLE temp.task_status(
  id integer,
  status varchar
);

INSERT INTO temp.task_status
SELECT
  id,
  upper(substring(status,1,1)) || lower(substring(status,2))
FROM
  tasks;

UPDATE
  tasks
SET
  status = temp.task_status.status
FROM
  temp.task_status
WHERE
  tasks.id = temp.task_status.id;

