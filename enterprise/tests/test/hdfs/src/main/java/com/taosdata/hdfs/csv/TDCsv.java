package com.taosdata.hdfs.csv;

import com.taosdata.hdfs.csv.internal.TDConfig;
import com.taosdata.hdfs.csv.internal.TDLog;
import com.taosdata.hdfs.csv.internal.TDUtil;

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;

public class TDCsv {
    public boolean parseStream(InputStreamReader infile) {
        TDLog.error("parseStream:%s is a abstract interface");
        return true;
    }

    protected long getBeginTs() {
        long beginTs = TDUtil.getTimeStampUs();
        while (beginTs <= TDConfig.fileBeginTimestampUs) {
            TDUtil.sleepMs(100);
            beginTs = TDUtil.getTimeStampUs();
        }
        return beginTs;
    }

    public boolean parseFile() {
        File infile = new File(this.fileName);
        try {
            return parseStream(new FileReader(infile));
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("failed to read file:%s, error:%s", this.fileName, e.getMessage()));
            return false;
        }
    }

    public synchronized void addInsertRows(int insertedRows) {
        this.insertedRows += insertedRows;
    }

    public void addInsertTimeSec(float insertTimeTs) {
        this.insertTimeTs += insertTimeTs;
    }

    public void addCreatetbTimeSec(float createtbTimeTs) {
        this.createtbTimeTs = createtbTimeTs;
    }

    public String getFileName() {
        return this.fileName;
    }

    public String getFileBaseName() {
        return this.fileBaseName;
    }

    public int getParsedRows() {
        return this.parsedRows;
    }

    public int getFetchedRows() {
        return lineIndex;
    }

    public int getInsertedRows() {
        return this.insertedRows;
    }

    public float getParseTimeSec() {
        return this.parseTimeTs;
    }

    public float getInsertTimeSec() {
        return this.insertTimeTs;
    }

    public float getCreatetbTimeSec() {
        return this.createtbTimeTs;
    }

    protected String fileName = "";
    protected String fileBaseName = "";
    protected  int lineIndex = 0;
    protected int parsedRows = 0;
    protected int insertedRows = 0;
    protected float parseTimeTs = 0;
    protected float insertTimeTs = 0;
    protected float createtbTimeTs = 0;
    public long beginTs = getBeginTs();
}
