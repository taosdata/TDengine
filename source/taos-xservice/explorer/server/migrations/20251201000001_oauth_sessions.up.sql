-- Create OAuth user mapping table
CREATE TABLE IF NOT EXISTS oauth_users(
    user_id integer PRIMARY KEY AUTOINCREMENT,
    provider text NOT NULL,
    username text NOT NULL,
    nickname text,
    email text,
    tsdb_username text,
    tsdb_password text,
    created_at integer timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at integer timestamp DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider, username)
);

-- Create OAuth user sessions table
CREATE TABLE IF NOT EXISTS oauth_sessions(
    session_id text PRIMARY KEY NOT NULL,
    user_id integer NOT NULL,
    access_token text,
    refresh_token text,
    id_token text,
    expires_at timestamp NOT NULL,
    login_at timestamp NOT NULL,
    last_active timestamp NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_oauth_sessions_username ON oauth_sessions(user_id);

CREATE INDEX IF NOT EXISTS idx_oauth_sessions_expires ON oauth_sessions(expires_at);

CREATE INDEX IF NOT EXISTS idx_oauth_sessions_last_active ON oauth_sessions(last_active);

-- Create OAuth configuration table
CREATE TABLE IF NOT EXISTS oauth_config(
    id integer PRIMARY KEY AUTOINCREMENT,
    provider_name text NOT NULL,
    enabled integer NOT NULL DEFAULT 0,
    client_id text,
    client_secret text,
    issuer_url text,
    authorization_endpoint text,
    token_endpoint text,
    userinfo_endpoint text,
    jwks_uri text,
    scopes text,
    redirect_uri text,
    user_mapping text,
    updated_at integer
);

