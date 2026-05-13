-- Add up migration script here
CREATE TABLE IF NOT EXISTS registration(
  id integer PRIMARY KEY AUTOINCREMENT,
  `subject` text NOT NULL,
  `cid` text NOT NULL,
  `version` text NOT NULL,
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (`subject`, `cid`)
);

