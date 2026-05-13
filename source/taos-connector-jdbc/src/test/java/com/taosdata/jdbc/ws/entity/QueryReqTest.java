package com.taosdata.jdbc.ws.entity;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Minimal JUnit4 test for QueryReq (no mock, JDK8 compatible)
 */
public class QueryReqTest {

    // Basic test data
    private static final String TEST_SQL = "SELECT * FROM test_table";

    @Test
    public void testSqlGetterAndSetter() {
        // 1. Initialize QueryReq (default constructor)
        QueryReq queryReq = new QueryReq();

        // 2. Test setter & getter with normal SQL string
        queryReq.setSql(TEST_SQL);
        assertEquals(TEST_SQL, queryReq.getSql());

        // 3. Test edge case: null SQL
        queryReq.setSql(null);
        assertNull(queryReq.getSql());

        // 4. Test edge case: empty SQL
        queryReq.setSql("");
        assertEquals("", queryReq.getSql());
    }
}