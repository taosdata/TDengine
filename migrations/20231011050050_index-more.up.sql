-- Add up migration script here

CREATE INDEX IF NOT EXISTS idx_tasks_name ON `tasks`(`name`);
CREATE INDEX IF NOT EXISTS idx_tasks_agent ON `tasks`(`via`);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON `tasks`(`created_at`);
