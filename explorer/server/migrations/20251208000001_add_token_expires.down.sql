-- Drop index
DROP INDEX IF EXISTS idx_oauth_sessions_token_expires;

-- Remove access_token_expires_at field from oauth_sessions table
ALTER TABLE oauth_sessions DROP COLUMN access_token_expires_at;
