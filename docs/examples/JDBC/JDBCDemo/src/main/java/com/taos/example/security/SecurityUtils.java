package com.taos.example.security;

import java.sql.SQLException;

/**
 * Utility class for security-related operations.
 * Provides common methods for token handling, connection building, and environment variables.
 */
public class SecurityUtils {

    // Nacos configuration constants
    public static final String DATA_ID = "tdengine-credential";
    public static final String GROUP = "DEFAULT_GROUP";

    // Token rotation configuration
    public static final long TOKEN_TTL_MS = 24 * 60 * 60 * 1000L;  // 24 hours
    public static final double ROTATION_THRESHOLD = 0.8;           // Rotate at 80% of TTL
    public static final long GRACE_PERIOD_MS = 5 * 60 * 1000L;     // 5 minutes grace period

    // Connection property constants
    public static final String PROP_TD_CONNECT_TYPE = "td.connect.type";
    public static final String PROP_TD_CONNECT_TOKEN = "td.connect.token";
    public static final String PROP_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String PROP_GROUP_ID = "group.id";
    public static final String PROP_AUTO_COMMIT = "enable.auto.commit";

    // Error code constants
    /** TDengine error code: authentication failure */
    public static final int TSDB_CODE_AUTH_FAILURE = 0x80000357;

    // Retry constants
    /** Retry delay (ms) when token rotation fails */
    public static final long ROTATION_RETRY_DELAY_MS = 60_000L;

    /**
     * Get environment variable with default value.
     */
    public static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Parse "token=<value>" from Nacos config content.
     */
    public static String parseToken(String content) {
        if (content == null) return "";
        for (String line : content.split("\n")) {
            line = line.trim();
            if (line.startsWith("token=")) {
                return line.substring("token=".length()).trim();
            }
        }
        return "";
    }

    /**
     * Mask token for logging (show first 10 chars only).
     */
    public static String maskToken(String token) {
        if (token == null || token.isEmpty()) return "";
        int visible = Math.min(10, token.length());
        return token.substring(0, visible) + "...";
    }

    /**
     * Build JDBC URL with SSL and token authentication.
     */
    public static String buildJdbcUrl(String host, int port, String db, String token) {
        return String.format(
                "jdbc:TAOS-WS://%s:%d/%s?bearerToken=%s&useSSL=true&varcharAsString=true",
                host, port, db, token);
    }

    /**
     * Build JDBC URL with custom parameters.
     */
    public static String buildJdbcUrl(String host, int port, String db, String token, boolean useSSL) {
        return String.format(
                "jdbc:TAOS-WS://%s:%d/%s?bearerToken=%s&useSSL=%b&varcharAsString=true",
                host, port, db, token, useSSL);
    }

    /**
     * Check if token should be rotated based on age.
     */
    public static boolean shouldRotateToken(long tokenCreateTime) {
        if (tokenCreateTime == 0) return false;
        long tokenAge = System.currentTimeMillis() - tokenCreateTime;
        return tokenAge >= (TOKEN_TTL_MS * ROTATION_THRESHOLD);
    }

    /**
     * Check if SQLException is an authentication/authorization error.
     */
    public static boolean isAuthError(SQLException e) {
        if (e == null) return false;
        String msg = e.getMessage();
        return msg != null && (
                msg.contains("auth") || msg.contains("Auth")
                || msg.contains("token") || msg.contains("Token")
                || msg.contains("Unauthorized")
                || msg.contains("SSL")
                || msg.contains("certificate")
                || e.getErrorCode() == TSDB_CODE_AUTH_FAILURE);
    }
}
