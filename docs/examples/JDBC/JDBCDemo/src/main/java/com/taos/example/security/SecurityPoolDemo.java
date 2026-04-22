package com.taos.example.security;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Scenario 1: Basic Token + SSL connection
 * Scenario 2: HikariCP connection pool with dynamic Token rotation
 *
 * Prerequisites: TDengine Enterprise with SSL enabled, root CA imported into
 * the client system trust store.
 *
 * Run (set env var first):
 *   export TDENGINE_TOKEN=your_token
 *   export TDENGINE_HOST=localhost
 *   cd docs/examples/JDBC/JDBCDemo
 *   mvn compile exec:java -Dexec.mainClass=com.taos.example.security.SecurityPoolDemo
 *
 * IMPORTANT:
 *   The value of TDENGINE_HOST must match a DNS/IP entry in the server certificate SAN,
 *   otherwise TLS handshake will fail.
 */
public class SecurityPoolDemo {

    private static final String HOST = SecurityUtils.getEnv("TDENGINE_HOST", "localhost");
    private static final int    PORT = Integer.parseInt(SecurityUtils.getEnv("TDENGINE_PORT", "6041"));
    private static final String DB   = SecurityUtils.getEnv("TDENGINE_DB", "mydb");

    // ANCHOR: basic-connect
    /** Current Token - updated by config center; volatile ensures visibility across threads */
    private static volatile String currentToken =
            System.getenv().getOrDefault("TDENGINE_TOKEN", "");

    /**
     * Scenario 1: basic secure connection.
     * - Token authentication (replaces username/password)
     * - useSSL=true enables transport encryption
     * - Root CA in system trust store, no cert path needed in code
     */
    public static void basicConnect() throws Exception {
        String url = SecurityUtils.buildJdbcUrl(HOST, PORT, DB, currentToken);

        try (Connection conn = DriverManager.getConnection(url);
             Statement  stmt = conn.createStatement();
             ResultSet  rs   = stmt.executeQuery("SELECT SERVER_VERSION()")) {
            if (rs.next()) {
                System.out.println("Connected. Server version: " + rs.getString(1));
            }
        }
    }
    // ANCHOR_END: basic-connect

    // ANCHOR: pool-config
    /**
     * Scenario 2a: create a HikariCP connection pool.
     *
     * Key parameter: maxLifetime = Token TTL / 2 (recommended range: 1/4 ~ 1/2).
     * Connections retire naturally at maxLifetime; new ones use the latest Token.
     */
    public static HikariDataSource createPool(long tokenTtlMs) {
        return createPool(tokenTtlMs, currentToken);
    }

    private static HikariDataSource createPool(long tokenTtlMs, String token) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(SecurityUtils.buildJdbcUrl(HOST, PORT, DB, token));
        cfg.setMaximumPoolSize(10);
        cfg.setMinimumIdle(2);
        cfg.setMaxLifetime(tokenTtlMs / 2);     // half of Token TTL; connections won't outlive the token
        cfg.setKeepaliveTime(60_000);            // keepalive probe every 60 s
        cfg.setConnectionTestQuery("SELECT SERVER_STATUS()");
        return new HikariDataSource(cfg);
    }
    // ANCHOR_END: pool-config

    // ANCHOR: token-refresh
    /**
     * Scenario 2b: Token rotation - called by the config-center callback.
     *
     * Strategy:
     *   1. Create a new pool with the new Token immediately (new requests go to new pool).
     *   2. Close the old pool - waits for in-flight connections then shuts down gracefully.
     * The two-pool overlap window is milliseconds; transparent to the application.
     */
    public static HikariDataSource rotatePool(
            HikariDataSource oldPool, String newToken, long tokenTtlMs) {
        if (newToken == null || newToken.trim().isEmpty()) {
            throw new IllegalArgumentException("newToken must not be null or empty");
        }
        HikariDataSource newPool = createPool(tokenTtlMs, newToken);
        currentToken = newToken;                 // update the global Token after the new pool is ready
        if (oldPool != null) {
            oldPool.close();                     // drain active connections, then close
        }
        return newPool;
    }
    // ANCHOR_END: token-refresh

    public static void main(String[] args) throws Exception {
        long ttlMs = 3_600_000L;  // example Token TTL = 1 hour
        HikariDataSource pool = createPool(ttlMs);
        try {
            try (Connection conn = pool.getConnection();
                 Statement  stmt = conn.createStatement();
                 ResultSet  rs   = stmt.executeQuery("SELECT SERVER_VERSION()")) {
                System.out.println("Pool OK. Server: " + (rs.next() ? rs.getString(1) : "?"));
            }
            // Simulate Token rotation (triggered by config-center watch/listener in production):
            // pool = rotatePool(pool, System.getenv("TDENGINE_TOKEN_NEW"), ttlMs);
        } finally {
            pool.close();
        }
    }
}
