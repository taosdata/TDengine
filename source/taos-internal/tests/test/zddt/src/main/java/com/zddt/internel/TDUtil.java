package com.zddt.internel;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class TDUtil {
    public static String getTimeStringMs() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return df.format(new Date());
    }

    public static long getTimeMsFromFormat(String data, String fmt) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date d;
        try {
            d = sdf.parse(data);
        } catch (Exception e) {
            //e.printStackTrace();
            return -1;
        }

        return d.getTime();
    }

    public static long getTimeMsFromYYYYMMDD(String yyyymmdd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date d;
        try {
            d = sdf.parse(yyyymmdd);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }

        return d.getTime();
    }

    public static long getTimeStampUs() {
        Long cutime = System.currentTimeMillis() * 1000;
        Long nanoTime = System.nanoTime();
        return cutime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }

    public static long getTimeStampMs() {
        return System.currentTimeMillis();
    }

    public static void sleepMs(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {

        }
    }

    public static String getBaseFileName(String fileName) {
        int begin = 0;
        int end = fileName.length();
        for (int i = end - 1; i >= 0; i--) {
            if (fileName.charAt(i) == '.') {
                end = i;
                break;
            }
        }

        for (int i = end - 1; i >= 0; i--) {
            if (fileName.charAt(i) == '\\' || fileName.charAt(i) == '/') {
                begin = i + 1;
                break;
            }
        }

        if (end < 0 || end >= fileName.length()) {
            end = fileName.length();
        }

        if (begin == end) {
            begin = end - 1;
        }

        if (begin < 0 || begin >= fileName.length()) {
            begin = 0;
        }

        String name = fileName.substring(begin, end);
        return name;
    }

    public static ArrayList<String> getAllFiles(String filepath) {
        File file = new File(filepath);
        File[] fileList = file.listFiles();
        ArrayList<String> allFiles = new ArrayList<String>();
        for (File f : fileList) {
            allFiles.add(f.getPath());
        }


        return allFiles;
    }

    public static String getAbsolutePath(String path) {
        File directory = new File("./");
        return directory.getAbsolutePath();
    }
}
