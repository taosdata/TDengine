package com.taosdata.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.TimeZone;

/**
 * 时间工具类
 *
 * @author ZYP
 */
public class DateUtils {

    protected static Logger logger = LoggerFactory.getLogger(DateUtils.class);

    /* 日期格式 */
    public static String DATE_FORMAT_01 = "yyyyMMddHHmmssS";
    public static String DATE_FORMAT_02 = "yyyyMMddHHmmss";
    public static String DATE_FORMAT_03 = "yyyyMMddHHmm";
    public static String DATE_FORMAT_04 = "yyyyMMddHH";
    public static String DATE_FORMAT_05 = "yyyyMMdd";
    public static String DATE_FORMAT_06 = "yyyyMM";
    public static String DATE_FORMAT_07 = "MMdd";
    public static String DATE_FORMAT_08 = "MM";
    public static String DATE_FORMAT_09 = "dd";
    public static String DATE_FORMAT_10 = "HHmmss";
    public static String DATE_FORMAT_11 = "HHmm";
    public static String DATE_FORMAT_12 = "HH";
    public static String DATE_FORMAT_13 = "mm";
    public static String DATE_FORMAT_14 = "ss";
    public static String DATE_FORMAT_15 = "yyyy-MM-dd HH:mm:ss";
    public static String DATE_FORMAT_16 = "yyyy/MM/dd HH:mm:ss";
    public static String DATE_FORMAT_17 = "yyyy-MM-dd";
    public static String DATE_FORMAT_18 = "yyyy/MM/dd";
    public static String DATE_FORMAT_19 = "yyyy-MM";
    public static String DATE_FORMAT_20 = "yyyy/MM";

    /* 年月日正则 */
    public static String PATTERN_YMD = "^((19|20)[0-9]{2})-((0?2-((0?[1-9])|([1-2][0-9])))|(0?(1|3|5|7|8|10|12)-((0?[1-9])|([1-2][0-9])|(3[0-1])))|(0?(4|6|9|11)-((0?[1-9])|([1-2][0-9])|30)))$";
    public static String PATTERN_YMDHMS = "^((19|20)[0-9]{2})-((0?2-((0?[1-9])|([1-2][0-9])))|(0?(1|3|5|7|8|10|12)-((0?[1-9])|([1-2][0-9])|(3[0-1])))|(0?(4|6|9|11)-((0?[1-9])|([1-2][0-9])|30)))\\s([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$";

    /**
     * 获得某一格式的当前时间
     *
     * @param dateFormat
     * @return
     */
    public static String getTime(String dateFormat) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        Date date = new Date();
        return simpleDateFormat.format(date);
    }

    /**
     * 获得某一格式&某一时区的当前时间
     *
     * @param dateFormat
     * @param timeZone
     * @return
     */
    public static String getTime(String dateFormat, TimeZone timeZone) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        Date date = new Date();
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.format(date);
    }

    /**
     * 日期转换字符串
     *
     * @param date
     * @param dateFormat
     * @return
     * @throws Exception
     */
    public static String dateToString(Date date, String dateFormat) throws Exception {
        return dateToString(date, dateFormat, TimeZone.getDefault());
    }

    /**
     * 日期转换字符串
     *
     * @param date
     * @param dateFormat
     * @param timeZone
     * @return
     * @throws Exception
     */
    public static String dateToString(Date date, String dateFormat, TimeZone timeZone) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        if (date == null) {
            throw new Exception("invalid date exception");
        }
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.format(date);
    }

    /**
     * 字符串转换日期
     *
     * @param date
     * @param dateFormat
     * @return
     * @throws Exception
     */
    public static Date stringToDate(String date, String dateFormat) throws Exception {
        return stringToDate(date, dateFormat, TimeZone.getDefault());
    }

    /**
     * 字符串转换日期
     *
     * @param date
     * @param dateFormat
     * @param timeZone
     * @return
     * @throws Exception
     */
    public static Date stringToDate(String date, String dateFormat, TimeZone timeZone) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        if (StringUtils.isEmpty(date)) {
            throw new Exception("invalid date exception");
        }
        simpleDateFormat.setTimeZone(timeZone);
        return simpleDateFormat.parse(date);
    }

    /**
     * 将OffsetDateTime转换为Date
     *
     * @param offsetDateTime
     * @return
     */
    public static Date fromOffsetDateTime(OffsetDateTime offsetDateTime) {
        try {
            // 当前时区的时间
            ZonedDateTime zonedDateTime = offsetDateTime.atZoneSameInstant(ZoneId.systemDefault());
            // 转换为Date并返回
            return Date.from(zonedDateTime.toInstant());
        } catch (Exception e) {
            logger.error("an exception occurred while transform 'OffsetDateTime' to 'Date', param: " + offsetDateTime, e);
            return new Date();
        }
    }

    /**
     * 将Date转换为OffsetDateTime
     *
     * @param date
     * @return
     */
    public static OffsetDateTime toOffsetDateTime(Date date) {
        try {
            // 转换为OffsetDateTime并返回
            return OffsetDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        } catch (Exception e) {
            logger.error("an exception occurred while transform 'OffsetDateTime' to 'Date', param: " + date, e);
            return OffsetDateTime.now();
        }
    }
}
