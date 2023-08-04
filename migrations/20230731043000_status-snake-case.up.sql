-- Add up migration script here
CREATE TABLE temp.task_status(
  id integer,
  status varchar
);

INSERT INTO temp.task_status
SELECT
  id,
  lower(status)
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

