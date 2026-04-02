package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.enums.TimestampPrecision;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class DateTimeUtilsTest {
    private static TimeZone originalTimeZone;
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final ZoneId NEW_YORK_ZONE = ZoneId.of("America/New_York");

    // Test data
    private static final String MILLISECOND_STRING = "2024-01-15 10:30:45.123";
    private static final String MICROSECOND_STRING = "2024-01-15 10:30:45.123456";
    private static final String NANOSECOND_STRING = "2024-01-15 10:30:45.123456789";

    @Test
    public void testParseTimeWithNullZone() {
        Time result = DateTimeUtils.parseTime(MILLISECOND_STRING, null);
        assertNotNull(result);
        assertEquals(Time.valueOf("10:30:45").getTime(), result.getTime());
    }

    @Test
    public void testParseTimeWithZone() {
        Time result = DateTimeUtils.parseTime(MILLISECOND_STRING, UTC_ZONE);
        assertNotNull(result);
        assertEquals(Time.valueOf("10:30:45").getTime(), result.getTime());
    }

    @Test
    public void testParseDateWithNullZone() {
        Date result = DateTimeUtils.parseDate(MILLISECOND_STRING, null);
        assertNotNull(result);
        assertEquals(Date.valueOf("2024-01-15").getTime(), result.getTime());
    }

    @Test
    public void testParseDateWithZone() {
        Date result = DateTimeUtils.parseDate(MILLISECOND_STRING, UTC_ZONE);
        assertNotNull(result);
        assertEquals(Date.valueOf("2024-01-15").getTime(), result.getTime());
    }

    @Test
    public void testParseTimestampWithNullZone() {
        Timestamp result = DateTimeUtils.parseTimestamp(MILLISECOND_STRING, null);
        assertNotNull(result);
        assertEquals(Timestamp.valueOf("2024-01-15 10:30:45.123").getTime(), result.getTime());
        assertEquals(123000000, result.getNanos());
    }

    @Test
    public void testParseTimestampWithZone() {
        Timestamp result = DateTimeUtils.parseTimestamp(MILLISECOND_STRING, UTC_ZONE);
        assertNotNull(result);
        assertEquals(Timestamp.valueOf("2024-01-15 10:30:45.123").getTime(), result.getTime());
        assertEquals(123000000, result.getNanos());
    }

    @Test
    public void testParseTimestampWithDifferentPrecisions() {
        // Millisecond precision
        Timestamp millisResult = DateTimeUtils.parseTimestamp(MILLISECOND_STRING, null);
        assertNotNull(millisResult);
        assertEquals(123000000, millisResult.getNanos());

        // Microsecond precision
        Timestamp microsResult = DateTimeUtils.parseTimestamp(MICROSECOND_STRING, null);
        assertNotNull(microsResult);
        assertEquals(123456000, microsResult.getNanos());

        // Nanosecond precision
        Timestamp nanosResult = DateTimeUtils.parseTimestamp(NANOSECOND_STRING, null);
        assertNotNull(nanosResult);
        assertEquals(123456789, nanosResult.getNanos());
    }

    @Test
    public void testToInstantWithTimestampAndNullZone() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Instant result = DateTimeUtils.toInstant(timestamp, (ZoneId) null);
        assertNotNull(result);
        assertEquals(timestamp.toInstant(), result);
    }

    @Test
    public void testToInstantWithTimestampAndZone() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Instant result = DateTimeUtils.toInstant(timestamp, UTC_ZONE);
        assertNotNull(result);
        assertEquals(timestamp.toInstant(), result);
    }

    @Test
    public void testToInstantWithTimestampAndCalendar() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(UTC_ZONE));
        Instant result = DateTimeUtils.toInstant(timestamp, cal);
        assertNotNull(result);
        assertEquals(timestamp.toInstant(), result);
    }

    @Test
    public void testToInstantWithNullTimestamp() {
        Instant result = DateTimeUtils.toInstant(null, UTC_ZONE);
        assertNull(result);
    }

    @Test
    public void testToLongWithInstantAndPrecision() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        Long millisResult = DateTimeUtils.toLong(instant, TimestampPrecision.MS);
        assertNotNull(millisResult);
        assertEquals(1705314645123L, millisResult.longValue());

        Long microsResult = DateTimeUtils.toLong(instant, TimestampPrecision.US);
        assertNotNull(microsResult);
        assertEquals(1705314645123000L, microsResult.longValue());

        Long nanosResult = DateTimeUtils.toLong(instant, TimestampPrecision.NS);
        assertNotNull(nanosResult);
        assertEquals(1705314645123000000L, nanosResult.longValue());
    }

    @Test
    public void testToLongWithTimestampAndZoneAndPrecision() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Long result = DateTimeUtils.toLong(timestamp, UTC_ZONE, TimestampPrecision.MS);
        assertNotNull(result);
        assertEquals(1705314645123L, result.longValue());
    }

    @Test
    public void testToLongWithNullTimestamp() {
        Long result = DateTimeUtils.toLong((Timestamp) null, UTC_ZONE, TimestampPrecision.MS);
        assertNull(result);
    }

    @Test
    public void testToLongWithLocalDateTime() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123000000);
        Long result = DateTimeUtils.toLong(localDateTime, UTC_ZONE, TimestampPrecision.MS);
        assertNotNull(result);
        assertEquals(1705314645123L, result.longValue());
    }

    @Test
    public void testToLongWithZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 123000000, UTC_ZONE);
        Long result = DateTimeUtils.toLong(zonedDateTime, TimestampPrecision.MS);
        assertNotNull(result);
        assertEquals(1705314645123L, result.longValue());
    }

    @Test
    public void testToLongWithOffsetDateTime() {
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 123000000, ZoneOffset.UTC);
        Long result = DateTimeUtils.toLong(offsetDateTime, TimestampPrecision.MS);
        assertNotNull(result);
        assertEquals(1705314645123L, result.longValue());
    }

    @Test
    public void testParseTimestampColumnData() {
        long millisValue = 1705314645123L;
        Instant millisInstant = DateTimeUtils.parseTimestampColumnData(millisValue, TimestampPrecision.MS);
        assertNotNull(millisInstant);
        assertEquals(millisValue, millisInstant.toEpochMilli());

        long microsValue = 1705314645123000L;
        Instant microsInstant = DateTimeUtils.parseTimestampColumnData(microsValue, TimestampPrecision.US);
        assertNotNull(microsInstant);
        assertEquals(microsValue / 1000_000L, microsInstant.getEpochSecond());
        assertEquals((microsValue % 1000_000L) * 1000L, microsInstant.getNano());

        long nanosValue = 1705314645123000000L;
        Instant nanosInstant = DateTimeUtils.parseTimestampColumnData(nanosValue, TimestampPrecision.NS);
        assertNotNull(nanosInstant);
        assertEquals(nanosValue / 1000_000_000L, nanosInstant.getEpochSecond());
        assertEquals(nanosValue % 1000_000_000L, nanosInstant.getNano());
    }

    @Test
    public void testParseTimestampColumnDataWithInvalidPrecision() {
        Instant result = DateTimeUtils.parseTimestampColumnData(1705314645123L, 999);
        assertNull(result);
    }

    @Test
    public void testGetTimestampFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        Timestamp resultWithNullZone = DateTimeUtils.getTimestamp(instant, null);
        assertNotNull(resultWithNullZone);
        assertEquals(Timestamp.from(instant).getTime(), resultWithNullZone.getTime());
        assertEquals(Timestamp.from(instant).getNanos(), resultWithNullZone.getNanos());

        Timestamp resultWithZone = DateTimeUtils.getTimestamp(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals(Timestamp.valueOf("2024-01-15 10:30:45.123").getTime(), resultWithZone.getTime());
        assertEquals(123000000, resultWithZone.getNanos());
    }

    @Test
    public void testGetLocalDateTimeFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        LocalDateTime resultWithNullZone = DateTimeUtils.getLocalDateTime(instant, null);
        assertNotNull(resultWithNullZone);
        // 检查是否为有效的 LocalDateTime
        assertEquals(2024, resultWithNullZone.getYear());
        assertEquals(1, resultWithNullZone.getMonthValue());
        assertEquals(15, resultWithNullZone.getDayOfMonth());

        LocalDateTime resultWithZone = DateTimeUtils.getLocalDateTime(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123000000), resultWithZone);
    }

    @Test
    public void testGetOffsetDateTimeFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        OffsetDateTime resultWithNullZone = DateTimeUtils.getOffsetDateTime(instant, null);
        assertNotNull(resultWithNullZone);
        // 检查是否为有效的 OffsetDateTime
        assertEquals(2024, resultWithNullZone.getYear());
        assertEquals(1, resultWithNullZone.getMonthValue());

        OffsetDateTime resultWithZone = DateTimeUtils.getOffsetDateTime(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals(OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 123000000, ZoneOffset.UTC), resultWithZone);
    }

    @Test
    public void testGetZonedDateTimeFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        ZonedDateTime resultWithNullZone = DateTimeUtils.getZonedDateTime(instant, null);
        assertNotNull(resultWithNullZone);
        // 检查是否为有效的 ZonedDateTime
        assertEquals(2024, resultWithNullZone.getYear());
        assertEquals(1, resultWithNullZone.getMonthValue());

        ZonedDateTime resultWithZone = DateTimeUtils.getZonedDateTime(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals(ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 123000000, UTC_ZONE), resultWithZone);
    }

    @Test
    public void testGetDateFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        Date resultWithNullZone = DateTimeUtils.getDate(instant, null);
        assertNotNull(resultWithNullZone);
        assertEquals("2024-01-15", resultWithNullZone.toString());

        Date resultWithZone = DateTimeUtils.getDate(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals("2024-01-15", resultWithZone.toString());
    }

    @Test
    public void testGetTimeFromInstant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");

        Time resultWithNullZone = DateTimeUtils.getTime(instant, null);
        assertNotNull(resultWithNullZone);
        assertEquals("10:30:45", resultWithNullZone.toString());

        Time resultWithZone = DateTimeUtils.getTime(instant, UTC_ZONE);
        assertNotNull(resultWithZone);
        assertEquals("10:30:45", resultWithZone.toString());

    }

    @Test
    public void testTimeZoneConversion() {
        // Test that timezone conversion works correctly
        String timestampStr = "2024-01-15 10:30:45.123";

        // Parse with UTC timezone
        Timestamp utcTimestamp = DateTimeUtils.parseTimestamp(timestampStr, UTC_ZONE);

        // Parse with New York timezone (UTC-5 in winter)
        Timestamp nyTimestamp = DateTimeUtils.parseTimestamp(timestampStr, NEW_YORK_ZONE);

        assertNotNull(utcTimestamp);
        assertNotNull(nyTimestamp);

        // Convert both to Instant and compare
        Instant utcInstant = DateTimeUtils.toInstant(utcTimestamp, UTC_ZONE);
        Instant nyInstant = DateTimeUtils.toInstant(nyTimestamp, NEW_YORK_ZONE);

        // The instants should be different because the same string is interpreted in different timezones
        assertNotEquals(utcInstant, nyInstant);
    }

    @Test(expected = java.time.format.DateTimeParseException.class)
    public void testParseInvalidTimestampString() {
        DateTimeUtils.parseTimestamp("invalid-timestamp-string", null);
    }

    @Test
    public void testEdgeCases() {
        // Test with minimum timestamp
        String minTimestamp = "1970-01-01 00:00:00.000";
        Timestamp result = DateTimeUtils.parseTimestamp(minTimestamp, UTC_ZONE);
        assertNotNull(result);
        assertEquals(Timestamp.valueOf(minTimestamp).getTime(), result.getTime());
        assertEquals(0, result.getNanos());

        // Test with maximum timestamp (near SQL Timestamp max)
        String maxTimestamp = "2038-01-19 03:14:07.999";
        Timestamp maxResult = DateTimeUtils.parseTimestamp(maxTimestamp, UTC_ZONE);
        assertNotNull(maxResult);
        assertEquals(Timestamp.valueOf(maxTimestamp).getTime(), maxResult.getTime());
    }

    @Test
    public void testCalendarWithNull() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Instant result = DateTimeUtils.toInstant(timestamp, (Calendar) null);
        assertNotNull(result);
        assertEquals(timestamp.toInstant(), result);
    }
    @Test
    public void testToLongNull() {
        assertNull(DateTimeUtils.toLong((Timestamp) null, null, TimestampPrecision.MS));
        assertNull(DateTimeUtils.toLong((LocalDateTime) null, null, TimestampPrecision.MS));
        assertNull(DateTimeUtils.toLong((ZonedDateTime) null, TimestampPrecision.MS));
    }

    // Set UTC timezone before all tests run
    @BeforeClass
    public static void setupTimeZone() {
        // Save original timezone (for restoration)
        originalTimeZone = TimeZone.getDefault();
        // Set JVM default timezone to UTC (JDK8 compatible)
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        // Verify the setting (optional)
        assertEquals(ZoneId.of("UTC"), ZoneId.systemDefault());
    }

    // Restore original timezone after all tests finish
    @AfterClass
    public static void restoreTimeZone() {
        if (originalTimeZone != null) {
            TimeZone.setDefault(originalTimeZone);
        }
    }

}