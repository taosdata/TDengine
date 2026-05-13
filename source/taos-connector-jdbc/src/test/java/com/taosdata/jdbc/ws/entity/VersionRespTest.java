package com.taosdata.jdbc.ws.entity;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Minimal JUnit4 test for VersionResp (no mock, JDK8 compatible)
 */
public class VersionRespTest {

    // Basic test data
    private static final String TEST_VERSION = "3.0.0";

    @Test
    public void testVersionGetterAndSetter() {
        // 1. Initialize VersionResp (default constructor)
        VersionResp versionResp = new VersionResp();

        // 2. Test setter & getter with normal version string
        versionResp.setVersion(TEST_VERSION);
        assertEquals(TEST_VERSION, versionResp.getVersion());

        // 3. Test edge case: null version
        versionResp.setVersion(null);
        assertNull(versionResp.getVersion());

        // 4. Test edge case: empty version string
        versionResp.setVersion("");
        assertEquals("", versionResp.getVersion());
    }
}