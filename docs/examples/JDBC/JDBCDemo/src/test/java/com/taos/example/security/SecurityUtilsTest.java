package com.taos.example.security;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for SecurityUtils.
 *
 * Note: These tests validate utility methods and logic.
 * Integration tests with actual TDengine/Nacos require environment setup.
 */
public class SecurityUtilsTest {

    private String validToken;

    @Before
    public void setUp() {
        validToken = "demo-token-REPLACE-ME-12345";
    }

    // -------------------------------------------------------------------------
    // Token parsing tests
    // -------------------------------------------------------------------------

    @Test
    public void testParseTokenValid() {
        String content = "token=demo-token-REPLACE-ME-12345";
        String result = SecurityUtils.parseToken(content);
        assertEquals("demo-token-REPLACE-ME-12345", result);
    }

    @Test
    public void testParseTokenMultiLine() {
        String content = "token=demo-token-REPLACE-ME-12345\nother=data";
        String result = SecurityUtils.parseToken(content);
        assertEquals("demo-token-REPLACE-ME-12345", result);
    }

    @Test
    public void testParseTokenEmpty() {
        assertEquals("", SecurityUtils.parseToken(null));
        assertEquals("", SecurityUtils.parseToken(""));
        assertEquals("", SecurityUtils.parseToken("no-token-here"));
    }

    // -------------------------------------------------------------------------
    // Token masking tests
    // -------------------------------------------------------------------------

    @Test
    public void testMaskTokenValid() {
        String result = SecurityUtils.maskToken(validToken);
        assertEquals("de...", result);
        assertTrue(result.endsWith("..."));
        assertEquals(5, result.length()); // 2 chars + "..."
    }

    @Test
    public void testMaskTokenShort() {
        String shortToken = "abc";
        String result = SecurityUtils.maskToken(shortToken);
        assertEquals("***", result);
    }

    @Test
    public void testMaskTokenLength4() {
        String token = "abcd";
        String result = SecurityUtils.maskToken(token);
        assertEquals("***", result);
    }

    @Test
    public void testMaskTokenEmpty() {
        assertEquals("", SecurityUtils.maskToken(null));
        assertEquals("", SecurityUtils.maskToken(""));
    }

    // -------------------------------------------------------------------------
    // Token rotation tests
    // -------------------------------------------------------------------------

    @Test
    public void testShouldRotateToken_False() {
        // Token just created (age = 0)
        long tokenCreateTime = System.currentTimeMillis();
        assertFalse(SecurityUtils.shouldRotateToken(tokenCreateTime));
    }

    @Test
    public void testShouldRotateToken_True() {
        // Token created 20 hours ago (80% of 24h TTL)
        long tokenCreateTime = System.currentTimeMillis() - (20 * 60 * 60 * 1000L);
        assertTrue(SecurityUtils.shouldRotateToken(tokenCreateTime));
    }

    @Test
    public void testShouldRotateToken_Boundary() {
        // Token created 19.2 hours ago (80% of 24h TTL = 19.2h)
        long tokenCreateTime = System.currentTimeMillis() - (long)(24 * 60 * 60 * 1000L * 0.8);
        assertTrue(SecurityUtils.shouldRotateToken(tokenCreateTime));
    }

    // -------------------------------------------------------------------------
    // JDBC URL building tests
    // -------------------------------------------------------------------------

    @Test
    public void testBuildJdbcUrl() {
        String url = SecurityUtils.buildJdbcUrl("localhost", 6041, "test_db", "test_token");
        assertTrue(url.contains("jdbc:TAOS-WS://"));
        assertTrue(url.contains("localhost:6041"));
        assertTrue(url.contains("test_db"));
        assertTrue(url.contains("bearerToken=test_token"));
        assertTrue(url.contains("useSSL=true"));
    }

    @Test
    public void testBuildJdbcUrlEncodeToken() {
        String url = SecurityUtils.buildJdbcUrl("localhost", 6041, "test_db", "a+b&c/d=");
        assertTrue(url.contains("bearerToken=a%2Bb%26c%2Fd%3D"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildJdbcUrlNullToken() {
        SecurityUtils.buildJdbcUrl("localhost", 6041, "test_db", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildJdbcUrlBlankToken() {
        SecurityUtils.buildJdbcUrl("localhost", 6041, "test_db", "   ");
    }

    @Test
    public void testGetEnv() {
        // Test default value when env var not set
        String result = SecurityUtils.getEnv("NON_EXISTENT_VAR_12345", "default_value");
        assertEquals("default_value", result);
    }

    @Test
    public void testIsAuthFailureByErrorCode() {
        java.sql.SQLException e = new java.sql.SQLException("any", "HY000", SecurityUtils.TSDB_CODE_AUTH_FAILURE);
        assertTrue(SecurityUtils.isAuthFailure(e));
    }

    @Test
    public void testIsAuthFailureByMessage() {
        java.sql.SQLException e = new java.sql.SQLException("auth failure: Invalid token", "HY000", 0);
        assertTrue(SecurityUtils.isAuthFailure(e));
    }

    @Test
    public void testIsAuthFailureNarrowFallback() {
        java.sql.SQLException e = new java.sql.SQLException("invalid token in SQL statement", "HY000", 0);
        assertFalse(SecurityUtils.isAuthFailure(e));
    }

    @Test
    public void testIsSecurityConnectErrorForTlsMessage() {
        java.sql.SQLException e = new java.sql.SQLException("SSL handshake failed: certificate verify failed", "HY000", 0);
        assertTrue(SecurityUtils.isSecurityConnectError(e));
    }
}
