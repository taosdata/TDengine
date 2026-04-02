package com.taosdata.jdbc.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimestampPrecisionTest {

    @Test
    public void testGetPrecision() {
        assertEquals(TimestampPrecision.MS, TimestampPrecision.getPrecision("ms"));
        assertEquals(TimestampPrecision.US, TimestampPrecision.getPrecision("us"));
        assertEquals(TimestampPrecision.NS, TimestampPrecision.getPrecision("ns"));
        assertEquals(TimestampPrecision.UNKNOWN, TimestampPrecision.getPrecision("unknown"));
        assertEquals(TimestampPrecision.UNKNOWN, TimestampPrecision.getPrecision(null));
    }

    @Test
    public void testGetPrecision_TrimInput() {
        assertEquals(TimestampPrecision.MS, TimestampPrecision.getPrecision(" ms "));
    }
}