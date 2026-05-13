package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.enums.TimestampPrecision;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

public class DateTimeUtils {
    private DateTimeUtils() {
    }

    private static final ZoneId systemZoneId = ZoneId.systemDefault();
    private static final DateTimeFormatter milliSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSS").toFormatter();
    private static final DateTimeFormatter microSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").toFormatter();
    private static final DateTimeFormatter nanoSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").toFormatter();

    public static Time parseTime(String timestampStr, ZoneId zoneId) throws DateTimeParseException {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr, zoneId);
        return dateTime != null ? Time.valueOf(dateTime.toLocalTime()) : null;
    }

    public static Date parseDate(String timestampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr, zoneId);
        return dateTime != null ? Date.valueOf(dateTime.toLocalDate()) : null;
    }

    public static Timestamp parseTimestamp(String timeStampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timeStampStr, zoneId);
        return dateTime != null ? Timestamp.valueOf(dateTime) : null;
    }

    private static LocalDateTime parseLocalDateTime(String timeStampStr, ZoneId zoneId) {
        try {
            return parseMilliSecTimestamp(timeStampStr, zoneId);
        } catch (DateTimeParseException e) {
            try {
                return parseMicroSecTimestamp(timeStampStr, zoneId);
            } catch (DateTimeParseException ee) {
                return parseNanoSecTimestamp(timeStampStr, zoneId);
            }
        }
    }

    private static LocalDateTime parseMilliSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null){
            return LocalDateTime.parse(timeStampStr, milliSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, milliSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    private static LocalDateTime parseMicroSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null) {
            return LocalDateTime.parse(timeStampStr, microSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, microSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    private static LocalDateTime parseNanoSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null) {
            return LocalDateTime.parse(timeStampStr, nanoSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, nanoSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    public static Instant toInstant(Timestamp timestamp, ZoneId zoneId) {
        if (timestamp == null) {
            return null;
        }

        if (zoneId == null) {
            return timestamp.toInstant();
        }

        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        return zonedDateTime.toInstant();
    }
    public static Instant toInstant(Timestamp timestamp, Calendar cal) {
        if (cal == null) {
            return timestamp.toInstant();
        }

        TimeZone calTimeZone = cal.getTimeZone();
        ZoneId zoneId = calTimeZone.toZoneId();

        return toInstant(timestamp, zoneId);
    }

    public static Long toLong(Instant instant, int precision) {
        long v;
        if (precision == TimestampPrecision.MS) {
            v = instant.toEpochMilli();
        } else if (precision == TimestampPrecision.US) {
            v = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        } else {
            v = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        }
        return v;
    }

    public static Long toLong(Timestamp timestamp,  ZoneId zoneId, int precision) {
        if (timestamp == null) {
            return null;
        }
        Instant instant = toInstant(timestamp, zoneId);
        return toLong(instant, precision);
    }

    public static Long toLong(LocalDateTime localDateTime,  ZoneId zoneId, int precision) {
        if (localDateTime == null) {
            return null;
        }
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        return toLong(zonedDateTime.toInstant(), precision);
    }

    public static Long toLong(ZonedDateTime zonedDateTime, int precision) {
        if (zonedDateTime == null) {
            return null;
        }
        return toLong(zonedDateTime.toInstant(), precision);
    }

    public static Long toLong(OffsetDateTime offsetDateTime, int precision) {
        if (offsetDateTime == null) {
            return null;
        }
        return toLong(offsetDateTime.toInstant(), precision);
    }



    public static Instant parseTimestampColumnData(long value, int timestampPrecision) {
        if (TimestampPrecision.MS == timestampPrecision)
            return Instant.ofEpochMilli(value);

        if (TimestampPrecision.US == timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Instant.ofEpochSecond(epochSec, nanoAdjustment);
        }
        if (TimestampPrecision.NS == timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Instant.ofEpochSecond(epochSec, nanoAdjustment);
        }
        return null;
    }

    public static Timestamp getTimestamp(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            return Timestamp.from(instant);
        }
        return Timestamp.valueOf(LocalDateTime.ofInstant(instant, zoneId));
    }

    public static LocalDateTime getLocalDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            ZonedDateTime zonedDateTime = instant.atZone(systemZoneId);
            return zonedDateTime.toLocalDateTime();
        }

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        return zonedDateTime.toLocalDateTime();
    }
    public static OffsetDateTime getOffsetDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            ZonedDateTime zonedDateTime = instant.atZone(systemZoneId);
            return zonedDateTime.toOffsetDateTime();
        }

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        return zonedDateTime.toOffsetDateTime();
    }

    public static ZonedDateTime getZonedDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            return instant.atZone(systemZoneId);
        }

        return instant.atZone(zoneId);
    }

    public static Date getDate(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            return new Date(instant.toEpochMilli());
        }

        Timestamp timestamp = getTimestamp(instant, zoneId);
        return new Date(timestamp.getTime());
    }
    public static Time getTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            return new Time(instant.toEpochMilli());
        }

        Timestamp timestamp = getTimestamp(instant, zoneId);
        return new Time(timestamp.getTime());
    }
}
