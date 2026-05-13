package com.taosdata.jdbc;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for VGroupIDResp class
 */
public class VGroupIDRespTest {

    private VGroupIDResp vGroupIDResp;

    /**
     * Initialize test instance before each test method
     */
    @Before
    public void setUp() {
        vGroupIDResp = new VGroupIDResp();
    }

    /**
     * Test default values of code and vgID
     */
    @Test
    public void testDefaultFieldValues() {
        assertEquals(0, vGroupIDResp.getCode());
        assertEquals(0, vGroupIDResp.getVgID());
    }

    /**
     * Test setCode() and getCode() methods with different values
     */
    @Test
    public void testSetAndGetCode() {
        // Test positive value
        int positiveCode = 200;
        vGroupIDResp.setCode(positiveCode);
        assertEquals(positiveCode, vGroupIDResp.getCode());

        // Test negative value
        int negativeCode = -50;
        vGroupIDResp.setCode(negativeCode);
        assertEquals(negativeCode, vGroupIDResp.getCode());

        // Test zero value
        int zeroCode = 0;
        vGroupIDResp.setCode(zeroCode);
        assertEquals(zeroCode, vGroupIDResp.getCode());
    }

    /**
     * Test setVgID() and getVgID() methods with different values
     */
    @Test
    public void testSetAndGetVgID() {
        // Test positive value
        int positiveVgID = 1000;
        vGroupIDResp.setVgID(positiveVgID);
        assertEquals(positiveVgID, vGroupIDResp.getVgID());

        // Test negative value
        int negativeVgID = -100;
        vGroupIDResp.setVgID(negativeVgID);
        assertEquals(negativeVgID, vGroupIDResp.getVgID());

        // Test zero value
        int zeroVgID = 0;
        vGroupIDResp.setVgID(zeroVgID);
        assertEquals(zeroVgID, vGroupIDResp.getVgID());
    }
}
