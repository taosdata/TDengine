package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.common.ConnectionParam;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Unit tests for ConnectReq (no mock frameworks, pure real-instance testing)
 * Compatible with JDK8 + JUnit4
 */
public class ConnectReqTest {

    // Test constants
    private static final String TEST_USER = "root";
    private static final String TEST_PASSWORD = "taosdata@123";
    private static final String TEST_DB = "test_db";
    private static final String TEST_TZ = "Asia/Shanghai";
    private static final String TEST_APP = "test_app";
    private static final String TEST_IP = "192.168.1.10";

    /**
     * Build a ConnectionParam instance for testing
     * @param connectMode Connection mode (e.g., CONNECT_MODE_BI)
     * @return Configured ConnectionParam instance
     */
    private ConnectionParam buildConnectionParam(int connectMode) {
        return new ConnectionParam.Builder(new ArrayList<>())
                .setUserAndPassword(TEST_USER, TEST_PASSWORD)
                .setDatabase(TEST_DB)
                .setTimeZone(TEST_TZ)
                .setAppName(TEST_APP)
                .setAppIp(TEST_IP)
                .setConnectMode(connectMode).build();
    }

    /**
     * Core test: Constructor + mode conversion logic for BI mode
     */
    @Test
    public void testConstructor_BI_Mode() {
        // 1. Prepare: Build ConnectionParam with BI mode
        ConnectionParam biParam = buildConnectionParam(ConnectionParam.CONNECT_MODE_BI);

        // 2. Execute: Create ConnectReq instance
        ConnectReq connectReq = new ConnectReq(biParam);

        // 3. Verify core logic
        // Check basic field assignment
        assertEquals("User should match test value", TEST_USER, connectReq.getUser());
        assertEquals("Password should match test value", TEST_PASSWORD, connectReq.getPassword());
        assertEquals("Database should match test value", TEST_DB, connectReq.getDb());
        assertEquals("Timezone should match test value", TEST_TZ, connectReq.getTz());
        assertEquals("App name should match test value", TEST_APP, connectReq.getApp());
        assertEquals("IP should match test value", TEST_IP, connectReq.getIp());

        // Check BI mode conversion (CONNECT_MODE_BI -> 0)
        assertEquals("BI mode should be converted to 0", Integer.valueOf(0), connectReq.getMode());

        // Check non-null values for static-dependent fields
        assertNotNull("ReqId should be generated (non-null)", connectReq.getReqId());
        assertNotNull("Connector version should be set (non-null)", connectReq.getConnector());
    }

    /**
     * Test: Constructor with non-BI mode (mode should be null)
     */
    @Test
    public void testConstructor_Non_BI_Mode() {
        // 1. Prepare: Build ConnectionParam with non-BI mode (custom mode 1)
        int nonBiMode = 1;
        ConnectionParam nonBiParam = buildConnectionParam(nonBiMode);

        // 2. Execute: Create ConnectReq instance
        ConnectReq connectReq = new ConnectReq(nonBiParam);

        // 3. Verify
        Assert.assertEquals(0, connectReq.getMode().intValue());
        // Ensure other fields are still correctly assigned
        assertEquals("User should still match test value", TEST_USER, connectReq.getUser());
    }

    /**
     * Test: Setter methods (verify value update)
     */
    @Test
    public void testSetterMethods() {
        // 1. Prepare: Create base ConnectReq instance
        ConnectionParam biParam = buildConnectionParam(ConnectionParam.CONNECT_MODE_BI);
        ConnectReq connectReq = new ConnectReq(biParam);

        // 2. Prepare new test values
        String newUser = "new_root";
        String newPassword = "new_pwd_456";
        String newDb = "new_test_db";
        Integer newMode = 2;
        String newTz = "UTC";
        String newApp = "new_test_app";
        String newIp = "10.0.0.1";
        String newConnector = "ws-connector-3.0.0";
        long newReqId = 123456789L;

        // 3. Execute: Update values via setters
        connectReq.setUser(newUser);
        connectReq.setPassword(newPassword);
        connectReq.setDb(newDb);
        connectReq.setMode(newMode);
        connectReq.setTz(newTz);
        connectReq.setApp(newApp);
        connectReq.setIp(newIp);
        connectReq.setConnector(newConnector);
        connectReq.setReqId(newReqId);

        // 4. Verify: Setters should update values correctly
        assertEquals("User should be updated", newUser, connectReq.getUser());
        assertEquals("Password should be updated", newPassword, connectReq.getPassword());
        assertEquals("Database should be updated", newDb, connectReq.getDb());
        assertEquals("Mode should be updated", newMode, connectReq.getMode());
        assertEquals("Timezone should be updated", newTz, connectReq.getTz());
        assertEquals("App name should be updated", newApp, connectReq.getApp());
        assertEquals("IP should be updated", newIp, connectReq.getIp());
        assertEquals("Connector should be updated", newConnector, connectReq.getConnector());
        assertEquals("ReqId should be updated", newReqId, connectReq.getReqId());
    }

    /**
     * Test: toPrintString() method (verify output format and sensitive data masking)
     */
    @Test
    public void testToPrintString() {
        // 1. Prepare: Create ConnectReq instance
        ConnectionParam biParam = buildConnectionParam(ConnectionParam.CONNECT_MODE_BI);
        ConnectReq connectReq = new ConnectReq(biParam);

        // 2. Execute: Get print string
        String printStr = connectReq.toPrintString();

        // 3. Verify: Output contains expected fields and masks password
        assertTrue("Print string should contain user", printStr.contains(TEST_USER));
        assertTrue("Print string should mask password with ******", printStr.contains("******"));
        assertTrue("Print string should contain database", printStr.contains(TEST_DB));
        assertTrue("Print string should contain mode 0", printStr.contains("mode=0"));
        assertTrue("Print string should contain timezone", printStr.contains(TEST_TZ));
        assertTrue("Print string should contain reqId", printStr.contains("reqId="));
        assertFalse("Print string should NOT contain plaintext password", printStr.contains(TEST_PASSWORD));
    }
}
