package com.taosdata.jdbc.utils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.taosdata.jdbc.enums.TimestampPrecision;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {

    private static final Pattern ptn = Pattern.compile(".*?'");
    private static final DateTimeFormatter milliSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSS").toFormatter();
    private static final DateTimeFormatter microSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").toFormatter();
    private static final DateTimeFormatter nanoSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").toFormatter();

    public static Time parseTime(String timestampStr) throws DateTimeParseException {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr);
        return dateTime != null ? Time.valueOf(dateTime.toLocalTime()) : null;
    }

    public static Date parseDate(String timestampStr) {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr);
        return dateTime != null ? Date.valueOf(String.valueOf(dateTime)) : null;
    }

    public static Timestamp parseTimestamp(String timeStampStr) {
        LocalDateTime dateTime = parseLocalDateTime(timeStampStr);
        return dateTime != null ? Timestamp.valueOf(dateTime) : null;
    }

    private static LocalDateTime parseLocalDateTime(String timeStampStr) {
        try {
            return parseMilliSecTimestamp(timeStampStr);
        } catch (DateTimeParseException e) {
            try {
                return parseMicroSecTimestamp(timeStampStr);
            } catch (DateTimeParseException ee) {
                return parseNanoSecTimestamp(timeStampStr);
            }
        }
    }

    private static LocalDateTime parseMilliSecTimestamp(String timeStampStr) throws DateTimeParseException {
        return LocalDateTime.parse(timeStampStr, milliSecFormatter);
    }

    private static LocalDateTime parseMicroSecTimestamp(String timeStampStr) throws DateTimeParseException {
        return LocalDateTime.parse(timeStampStr, microSecFormatter);
    }

    private static LocalDateTime parseNanoSecTimestamp(String timeStampStr) throws DateTimeParseException {
        return LocalDateTime.parse(timeStampStr, nanoSecFormatter);
    }

    public static String escapeSingleQuota(String origin) {
        Matcher m = ptn.matcher(origin);
        StringBuilder sb = new StringBuilder();
        int end = 0;
        while (m.find()) {
            end = m.end();
            String seg = origin.substring(m.start(), end);
            int len = seg.length();
            if (len == 1) {
                if ('\'' == seg.charAt(0)) {
                    sb.append("\\'");
                } else {
                    sb.append(seg);
                }
            } else { // len > 1
                sb.append(seg, 0, seg.length() - 2);
                char lastcSec = seg.charAt(seg.length() - 2);
                if (lastcSec == '\\') {
                    sb.append("\\'");
                } else {
                    sb.append(lastcSec);
                    sb.append("\\'");
                }
            }
        }

        if (end < origin.length()) {
            sb.append(origin.substring(end));
        }
        return sb.toString();
    }

    public static String getNativeSql(String rawSql, Object[] parameters) {
        if (parameters == null || !rawSql.contains("?"))
            return rawSql;
        // toLowerCase
        String preparedSql = rawSql.trim().toLowerCase();
        String[] clause = new String[]{"tags\\s*\\([\\s\\S]*?\\)", "where[\\s\\S]*"};
        Map<Integer, Integer> placeholderPositions = new HashMap<>();
        RangeSet<Integer> clauseRangeSet = TreeRangeSet.create();
        findPlaceholderPosition(preparedSql, placeholderPositions);
        // find tags and where clause's position
        findClauseRangeSet(preparedSql, clause, clauseRangeSet);
        // find values clause's position
        findValuesClauseRangeSet(preparedSql, clauseRangeSet);

        return transformSql(rawSql, parameters, placeholderPositions, clauseRangeSet);
    }

    private static void findValuesClauseRangeSet(String preparedSql, RangeSet<Integer> clauseRangeSet) {
        Matcher matcher = Pattern.compile("(values||,)\\s*(\\([^)]*\\))").matcher(preparedSql);
        while (matcher.find()) {
            int start = matcher.start(2);
            int end = matcher.end(2);
            clauseRangeSet.add(Range.closedOpen(start, end));
        }
    }

    private static void findClauseRangeSet(String preparedSql, String[] regexArr, RangeSet<Integer> clauseRangeSet) {
        clauseRangeSet.clear();
        for (String regex : regexArr) {
            Matcher matcher = Pattern.compile(regex).matcher(preparedSql);
            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                clauseRangeSet.add(Range.closedOpen(start, end));
            }
        }
    }

    private static void findPlaceholderPosition(String preparedSql, Map<Integer, Integer> placeholderPosition) {
        placeholderPosition.clear();
        Matcher matcher = Pattern.compile("\\?").matcher(preparedSql);
        int index = 0;
        while (matcher.find()) {
            int pos = matcher.start();
            placeholderPosition.put(index, pos);
            index++;
        }
    }

    /***
     *
     * @param rawSql
     * @param paramArr
     * @param placeholderPosition
     * @param clauseRangeSet
     * @return
     */
    private static String transformSql(String rawSql, Object[] paramArr, Map<Integer, Integer> placeholderPosition, RangeSet<Integer> clauseRangeSet) {
        String[] sqlArr = rawSql.split("\\?");

        return IntStream.range(0, sqlArr.length).mapToObj(index -> {
            if (index == paramArr.length)
                return sqlArr[index];

            Object para = paramArr[index];
            String paraStr;
            if (para != null) {
                if (para instanceof byte[]) {
                    paraStr = new String((byte[]) para, StandardCharsets.UTF_8);
                } else {
                    paraStr = para.toString();
                }
                // if para is timestamp or String or byte[] need to translate ' character
                if (para instanceof Timestamp || para instanceof String || para instanceof byte[]) {
                    paraStr = Utils.escapeSingleQuota(paraStr);

                    Integer pos = placeholderPosition.get(index);
                    boolean contains = clauseRangeSet.contains(pos);
                    if (contains) {
                        paraStr = "'" + paraStr + "'";
                    }
                }
            } else {
                paraStr = "NULL";
            }
            return sqlArr[index] + paraStr;
        }).collect(Collectors.joining());
    }

    public static String formatTimestamp(Timestamp timestamp) {
        int nanos = timestamp.getNanos();
        if (nanos % 1000000L != 0)
            return timestamp.toLocalDateTime().format(microSecFormatter);
        return timestamp.toLocalDateTime().format(milliSecFormatter);
    }

    public static TimestampPrecision guessTimestampPrecision(String value) {
        if (isMilliSecFormat(value))
            return TimestampPrecision.MS;
        if (isMicroSecFormat(value))
            return TimestampPrecision.US;
        if (isNanoSecFormat(value))
            return TimestampPrecision.NS;
        return TimestampPrecision.UNKNOWN;
    }

    private static boolean isMilliSecFormat(String timestampStr) {
        try {
            milliSecFormatter.parse(timestampStr);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    private static boolean isMicroSecFormat(String timestampStr) {
        try {
            microSecFormatter.parse(timestampStr);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    private static boolean isNanoSecFormat(String timestampStr) {
        try {
            nanoSecFormatter.parse(timestampStr);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }
}
