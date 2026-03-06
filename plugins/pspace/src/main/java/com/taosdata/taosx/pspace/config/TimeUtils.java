package com.taosdata.taosx.pspace.config;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Utility for parsing ISO-8601 datetime strings to epoch millis.
 */
public class TimeUtils {

    /**
     * Parse ISO-8601 datetime string to epoch milliseconds.
     *
     * @param isoDateTime ISO-8601 string, e.g. "2026-01-01T00:00:00+08:00"
     * @return epoch millis
     * @throws DateTimeParseException if the string cannot be parsed
     */
    public static long parseToEpochMillis(String isoDateTime) {
        return OffsetDateTime.parse(isoDateTime).toInstant().toEpochMilli();
    }

    /**
     * Parse ISO-8601 datetime string to epoch millis, returning null if input is
     * null or empty.
     */
    public static Long parseToEpochMillisOrNull(String isoDateTime) {
        if (isoDateTime == null || isoDateTime.isEmpty()) {
            return null;
        }
        return parseToEpochMillis(isoDateTime);
    }
}
