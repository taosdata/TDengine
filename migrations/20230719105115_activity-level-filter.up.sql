-- Add up migration script here
ALTER TABLE agent_activities
-- 3 = info
  ADD COLUMN `level` tinyint NOT NULL DEFAULT 3;

ALTER TABLE agents
  DROP COLUMN `status`;

ALTER TABLE task_activities
  ADD COLUMN `level` tinyint NOT NULL DEFAULT 3;

ALTER TABLE task_activities
  ADD COLUMN `status` varchar NOT NULL DEFAULT "ok";

CREATE VIEW IF NOT EXISTS `agents_view` AS
  SELECT * FROM agents LEFT JOIN (

SELECT
    DISTINCT id,             -- Only unique rows
    LAST_VALUE(status) OVER (    -- The last value of the status column
        PARTITION BY id      -- Taking into account rows with the same value in the object column
        ORDER by `at` ASC        -- "Last" when sorting the rows of every object by the time column in ascending order
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING    -- Take all rows in the patition
    ) as status
FROM
    agent_activities ) s ON (agents.id = s.id);
