package com.zddt.internel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class TDLog {
    public static synchronized boolean init(String _logDir, int _debugFlag, long _numOfLines) {
        logDir = _logDir;
        debugFlag = _debugFlag;
        numOfLines = _numOfLines;
        isPrintScreen = ((debugFlag & DEBUG_SCREEN) != 0);
        isPrintTrace = ((debugFlag & DEBUG_TRACE) != 0);
        if (!isInitialized) {
            initLogFile();
        }

        return true;
    }

    public static synchronized void error(String line) {
        try {
            String log = TDUtil.getTimeStringMs() + " ERROR " + line + "\r\n";
            if (isInitialized) {
                out.write(log);
                out.flush();
            }
            if (isPrintScreen) {
                System.out.print(log);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        check();
    }

    public static synchronized void print(String line) {
        try {
            String log = TDUtil.getTimeStringMs() + " PRINT " + line + "\r\n";
            if (isInitialized) {
                out.write(log);
                out.flush();
            }
            if (isPrintScreen) {
                System.out.print(log);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        check();
    }

    public static synchronized void trace(String line) {
        if (!isPrintTrace) {
            return;
        }

        try {
            String log = TDUtil.getTimeStringMs() + " TRACE " + line + "\r\n";
            if (isInitialized) {
                out.write(log);
                out.flush();
            }
            if (isPrintScreen) {
                System.out.print(log);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        check();
    }

    private static synchronized void check() {
        if (curLines++ > numOfLines) {
            initLogFile();
            TDLog.print("new log file created");
            ;
        }
    }

    private static synchronized void initLogFile() {
        curLines = 0;

        if (logDir == null || logDir.length() < 1) {
            return;
        }

        String fileName = String.format("%s/hdfs%d.log", logDir, curFileIndex);
        File outFile = new File(fileName);
        curFileIndex++;
        if (curFileIndex >= 2) {
            curFileIndex = 0;
        }

        try {
            outFile.createNewFile();
            out = new BufferedWriter(new FileWriter(outFile));
            isInitialized = true;
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("failed to create log file:%s", fileName));
        }
    }

    private static String logDir = "";
    private static int debugFlag = 199;
    private static long numOfLines = 100000;
    private static long curLines = 0;
    private static int curFileIndex = 0;
    private static boolean isInitialized = false;
    private static boolean isPrintScreen = true;
    private static boolean isPrintTrace = true;
    private static BufferedWriter out = null;
    private static int DEBUG_ERROR = 1;
    private static int DEBUG_WARN = 2;
    private static int DEBUG_TRACE = 4;
    private static int DEBUG_DUMP = 8;
    private static int DEBUG_SCREEN = 64;
    private static int DEBUG_FILE = 128;
}
