-- taosx 专用 Token 存储（用户启用 TOTP 时自动创建）
CREATE TABLE IF NOT EXISTS taosx_tokens(
    username TEXT PRIMARY KEY NOT NULL,       -- TDengine 用户名
    token_name TEXT NOT NULL,                 -- TSDB 中的 Token 名称 (__taosx_<username>__)
    encrypted_token TEXT NOT NULL,            -- AES-256-GCM 加密后的 Token 字符串
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
