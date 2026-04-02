package com.taosdata.jdbc.ws.entity;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Minimal JUnit4 test for Request (no mock, JDK8 compatible)
 */
public class RequestTest {

    // Basic test data
    private static final String TEST_ACTION = "tmq_fetch";
    private static final Long TEST_REQ_ID = 987654L;

    @Test
    public void testBasicFunctionality() {
        // 1. Prepare Payload (real instance, no mock)
        Payload payload = new Payload();
        payload.setReqId(TEST_REQ_ID);

        // 2. Test constructor & getter
        Request request = new Request(TEST_ACTION, payload);
        assertEquals(TEST_ACTION, request.getAction());
        assertEquals(payload, request.getArgs());
        assertEquals(TEST_REQ_ID, request.id()); // Test id() method

        // 3. Test setter
        String newAction = "tmq_ack";
        Long newReqId = 111222L;
        Payload newPayload = new Payload();
        newPayload.setReqId(newReqId);

        request.setAction(newAction);
        request.setArgs(newPayload);
        assertEquals(newAction, request.getAction());
        assertEquals(newReqId, request.id()); // Verify id() after setter

        // 4. Simple test for toString() (non-empty)
        String requestStr = request.toString();
        assertFalse("toString() should return non-empty string", requestStr.isEmpty());
    }
}