-- Add access_token_expires_at field to oauth_sessions table
ALTER TABLE oauth_sessions ADD COLUMN access_token_expires_at timestamp;

-- Create index for efficient token expiration queries
CREATE INDEX IF NOT EXISTS idx_oauth_sessions_token_expires ON oauth_sessions(access_token_expires_at);
