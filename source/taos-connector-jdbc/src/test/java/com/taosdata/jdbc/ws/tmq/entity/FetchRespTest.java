package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class FetchRespTest {

    private FetchResp fetchResp;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Test data constants
    private static final long TEST_MESSAGE_ID = 1234567890123456789L;
    private static final boolean TEST_COMPLETED = true;
    private static final String TEST_TABLE_NAME = "test_table";
    private static final int TEST_ROWS = 100;
    private static final int TEST_FIELDS_COUNT = 3;
    private static final String[] TEST_FIELDS_NAMES = {"id", "name", "value"};
    private static final int[] TEST_FIELDS_TYPES = {1, 2, 3};
    private static final long[] TEST_FIELDS_LENGTHS = {8, 256, 16};
    private static final int TEST_PRECISION = 6;

    // Test JSON string (matches Jackson annotations)
    private static final String TEST_JSON = "{" +
            "\"message_id\":\"1234567890123456789\"," +  // UInt64 as string (for UInt64Deserializer)
            "\"completed\":true," +
            "\"table_name\":\"test_table\"," +
            "\"rows\":100," +
            "\"fields_count\":3," +
            "\"fields_names\":[\"id\",\"name\",\"value\"]," +
            "\"fields_types\":[1,2,3]," +
            "\"fields_lengths\":[8,256,16]," +
            "\"precision\":6," +
            "\"code\":0," +  // Inherited from CommonResp
            "\"message\":\"success\"" +  // Inherited from CommonResp
            "}";

    @Before
    public void setUp() {
        // Initialize test instance before each test
        fetchResp = new FetchResp();
    }

    /**
     * Test all setter and getter methods
     */
    @Test
    public void testGetterAndSetter() {
        // Set all properties
        fetchResp.setMessageId(TEST_MESSAGE_ID);
        fetchResp.setCompleted(TEST_COMPLETED);
        fetchResp.setTableName(TEST_TABLE_NAME);
        fetchResp.setRows(TEST_ROWS);
        fetchResp.setFieldsCount(TEST_FIELDS_COUNT);
        fetchResp.setFieldsNames(TEST_FIELDS_NAMES);
        fetchResp.setFieldsTypes(TEST_FIELDS_TYPES);
        fetchResp.setFieldsLengths(TEST_FIELDS_LENGTHS);
        fetchResp.setPrecision(TEST_PRECISION);

        // Verify getters
        assertEquals(TEST_MESSAGE_ID, fetchResp.getMessageId());
        assertEquals(TEST_COMPLETED, fetchResp.isCompleted());
        assertEquals(TEST_TABLE_NAME, fetchResp.getTableName());
        assertEquals(TEST_ROWS, fetchResp.getRows());
        assertEquals(TEST_FIELDS_COUNT, fetchResp.getFieldsCount());
        assertArrayEquals(TEST_FIELDS_NAMES, fetchResp.getFieldsNames());
        assertArrayEquals(TEST_FIELDS_TYPES, fetchResp.getFieldsTypes());
        assertArrayEquals(TEST_FIELDS_LENGTHS, fetchResp.getFieldsLengths());
        assertEquals(TEST_PRECISION, fetchResp.getPrecision());

        // Test null values for reference types
        fetchResp.setTableName(null);
        fetchResp.setFieldsNames(null);
        fetchResp.setFieldsTypes(null);
        fetchResp.setFieldsLengths(null);

        assertNull(fetchResp.getTableName());
        assertNull(fetchResp.getFieldsNames());
        assertNull(fetchResp.getFieldsTypes());
        assertNull(fetchResp.getFieldsLengths());
    }

    /**
     * Test JSON deserialization (verify Jackson annotations work correctly)
     */
    @Test
    public void testJsonDeserialization() throws Exception {
        // Deserialize JSON to FetchResp
        FetchResp deserialized = objectMapper.readValue(TEST_JSON, FetchResp.class);

        // Verify deserialized properties
        assertEquals(TEST_MESSAGE_ID, deserialized.getMessageId());
        assertEquals(TEST_COMPLETED, deserialized.isCompleted());
        assertEquals(TEST_TABLE_NAME, deserialized.getTableName());
        assertEquals(TEST_ROWS, deserialized.getRows());
        assertEquals(TEST_FIELDS_COUNT, deserialized.getFieldsCount());
        assertArrayEquals(TEST_FIELDS_NAMES, deserialized.getFieldsNames());
        assertArrayEquals(TEST_FIELDS_TYPES, deserialized.getFieldsTypes());
        assertArrayEquals(TEST_FIELDS_LENGTHS, deserialized.getFieldsLengths());
        assertEquals(TEST_PRECISION, deserialized.getPrecision());

        // Verify inherited CommonResp properties (adjust based on actual CommonResp structure)
        assertEquals(0, deserialized.getCode()); // Assume CommonResp has getCode()
        assertEquals("success", deserialized.getMessage()); // Assume CommonResp has getMsg()
    }

    /**
     * Test edge cases: empty arrays and default values
     */
    @Test
    public void testEdgeCases() {
        // Test empty arrays
        String[] emptyFieldsNames = new String[0];
        int[] emptyFieldsTypes = new int[0];
        long[] emptyFieldsLengths = new long[0];

        fetchResp.setFieldsNames(emptyFieldsNames);
        fetchResp.setFieldsTypes(emptyFieldsTypes);
        fetchResp.setFieldsLengths(emptyFieldsLengths);

        assertArrayEquals(emptyFieldsNames, fetchResp.getFieldsNames());
        assertArrayEquals(emptyFieldsTypes, fetchResp.getFieldsTypes());
        assertArrayEquals(emptyFieldsLengths, fetchResp.getFieldsLengths());

        // Test default values (primitive types)
        FetchResp defaultResp = new FetchResp();
        assertEquals(0L, defaultResp.getMessageId()); // long default
        assertEquals(false, defaultResp.isCompleted()); // boolean default
        assertEquals(0, defaultResp.getRows()); // int default
        assertEquals(0, defaultResp.getFieldsCount()); // int default
        assertEquals(0, defaultResp.getPrecision()); // int default

        // Reference types default to null
        assertNull(defaultResp.getTableName());
        assertNull(defaultResp.getFieldsNames());
        assertNull(defaultResp.getFieldsTypes());
        assertNull(defaultResp.getFieldsLengths());
    }
}