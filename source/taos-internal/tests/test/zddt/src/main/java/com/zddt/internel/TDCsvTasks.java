package com.zddt.internel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class TDCsvTasks {
    public static void run() {
        if (TDConfig.localFile != "") {
            TDLog.print(String.format("task:0, file:%s start to run", TDConfig.localFile));
            TDCsvTask task = new TDCsvTask(0, TDConfig.localFile);
            TDInsertThreads insertThreads = new TDInsertThreads(0, task);
            insertThreads.run();
            TDLog.print(String.format("task:0, file:%s run finished", TDConfig.localFile));
        } else if (TDConfig.localDir != "") {
            ArrayList<String> allFiles = TDUtil.getAllFiles(TDConfig.localDir);
            TDLog.print(String.format("dir:%s, %d files will be disposed", TDConfig.localDir, allFiles.size()));
            for (int i = 0; i < allFiles.size(); ++i) {
                String fileName = allFiles.get(i);
                TDLog.print(String.format("task:%d, file:%s start to run", i, fileName));
                TDCsvTask task = new TDCsvTask(i, fileName);
                TDInsertThreads insertThreads = new TDInsertThreads(0, task);
                insertThreads.run();
                TDLog.print(String.format("task:%d, file:%s run finished", i, fileName));
            }
        } else {
        }
    }
}

class TDCsvTask extends TDTask {
    private BufferedReader br;
    InputStreamReader infile;
    private long startTs = 0;
    private int lineIndex = 0;

    public TDCsvTask(int taskIndex, String fileName) {
        this.taskIndex = taskIndex;
        this.taskName = fileName;
    }

    public boolean init() {
        startTs = TDUtil.getTimeStampMs();
        File file = new File(this.taskName);
        try {
            infile = new FileReader(file);
            br = new BufferedReader(infile);
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("task:%d, failed to read file:%s, error:%s", this.taskIndex, this.taskName, e.getMessage()));
            return false;
        } finally {
            TDLog.print(String.format("task:%d, file:%s, open success", this.taskIndex, this.taskName));
            return true;
        }
    }

    public TDLine getNextLine() throws Exception {
        String line = null;
        while ((line = br.readLine()) != null) {
            TDLine csvLine = new TDLine(++lineIndex, this);
            if (!csvLine.parse(line)) {
                continue;
            }

            fetchedRows++;
            return csvLine;
        }

        TDLog.print(String.format("task:%d, file:%s, fetch to the end", this.taskIndex, this.taskName));
        return null;
    }

    public void close() {
        fetchedTimeTs = (float) (TDUtil.getTimeStampMs() - startTs) / 1000;
        try {
            br.close();
            infile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        TDLog.print(String.format("task:%d, file:%s, handle successfully, totalReadLines:%d, totalParsedLines:%d, totalInsertLines:%d"
                , this.taskIndex, this.taskName, lineIndex, fetchedRows, insertedRows));
        TDLogDb.recordTask(this);
    }
}