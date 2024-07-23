-- Add up migration script here
CREATE TABLE IF NOT EXISTS sql_favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    sql TEXT NOT NULL,
    description TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_public BOOLEAN DEFAULT FALSE,
    UNIQUE(username, sql)
);

CREATE UNIQUE INDEX IF NOT EXISTS public_sql ON sql_favorites (sql) WHERE is_public = true;
