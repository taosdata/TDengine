package com.taosdata.hdfs.csv;

import com.taosdata.hdfs.csv.internal.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDCsvDirect extends TDCsv {
    private TDCsvDirectThread[] threads = null;
    private ExecutorService executorService = null;

    public TDCsvDirect(String fileName) {
        this.fileName = fileName;
        this.fileBaseName = TDUtil.getBaseFileName(this.fileName);
        this.initThreads();
    }

    private void initThreads() {
        this.threads = new TDCsvDirectThread[TDConfig.csvThreadNum];
        for (int threadIndex = 0; threadIndex < TDConfig.csvThreadNum; ++threadIndex) {
            TDCsvDirectThread thread = new TDCsvDirectThread(threadIndex, this);
            this.threads[threadIndex] = thread;
        }
    }

    private void runThreads() {
        TDLog.print(String.format("file:%s, %d threads start to run", this.fileName, threads.length));
        executorService = Executors.newFixedThreadPool(this.threads.length);
        for (TDCsvDirectThread thread : this.threads) {
            executorService.execute(thread);
        }
    }

    private void waitThreads() {
        for (TDCsvDirectThread thread : this.threads) {
            thread.setStopFlag();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        TDLog.print(String.format("file:%s, %d threads run finished", this.fileName, threads.length));
    }

    public boolean parseStream(InputStreamReader infile) {
        this.runThreads();

        BufferedReader br = new BufferedReader(infile);
        lineIndex = 0;
        try {
            TDLog.print(String.format("file:%s, open success", this.fileName));

            String line = null;
            long startTs = TDUtil.getTimeStampMs();

            while ((line = br.readLine()) != null) {
                if (lineIndex == 0 && TDConfig.ignoreFirstLine) {
                    lineIndex++;
                    continue;
                }

                TDCsvLine csvLine = new TDCsvLine(++lineIndex, this);
                if (!csvLine.parse(line)) {
                    continue;
                }

                TDTable tb = TDDataDb.getTbThread(csvLine);
                if (TDConfig.discardOldData) {
                    if (csvLine.timestamp <= tb.lastTimestamp) {
                        continue;
                    }
                    else {
                        tb.lastTimestamp = csvLine.timestamp;
                    }
                }

                threads[tb.threadIndex].addLine(csvLine);
                parsedRows++;
            }

            parseTimeTs += (float) (TDUtil.getTimeStampMs() - startTs) / 1000;
            TDLog.print(String.format("file:%s, totalReadLines:%d, totalParsedLines:%d", this.fileName, lineIndex, parsedRows));
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("file:%s, processing failed, error:%s", this.fileName, e.getMessage()));
            return false;
        } finally {
            this.waitThreads();
            TDLog.print(String.format("file:%s, handle successfully, totalReadLines:%d, totalParsedLines:%d, totalInsertLines:%d"
                    , this.fileName, lineIndex, parsedRows, insertedRows));
            TDLogDb.recordFile(this);

            try {
                br.close();
                infile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }
}

class TDCsvDirectThread implements Runnable {
    private int threadIndex;
    private boolean stopFlag;
    private TDCsvLine[] lines;
    private int maxLines;
    private int readPos;
    private int writePos;
    private int insertedRows;
    private TDCsv csv;
    private TDConnection connection = null;

    public TDCsvDirectThread(int threadIndex, TDCsv csv) {
        this.threadIndex = threadIndex;
        this.csv = csv;
        this.maxLines = TDConfig.fileCacheRows / TDConfig.csvThreadNum;
        this.lines = new TDCsvLine[this.maxLines];
        this.readPos = 0;
        this.writePos = 0;
        this.stopFlag = false;
        this.insertedRows = 0;
        this.connection = TDDataDb.getConnection(threadIndex);
    }

    public void setStopFlag() {
        stopFlag = true;
    }

    public void addLine(TDCsvLine line) {
        while (writePos - readPos >= maxLines) {
            TDUtil.sleepMs(100);
            continue;
        }
        lines[writePos % maxLines] = line;
        writePos++;
    }

    public void run() {
        TDLog.print(String.format("file:%s, thread:%d start to run", csv.fileName, threadIndex));
        while (true) {
            if (readPos >= writePos) {
                if (stopFlag) {
                    break;
                } else {
                    TDUtil.sleepMs(100);
                    continue;
                }
            }

            doInsert();
        }

        csv.addInsertRows(insertedRows);
        TDLog.print(String.format("file:%s, thread:%d run finished", csv.fileName, threadIndex));
    }

    private void doInsert() {
        StringBuilder sqlBuffer = new StringBuilder();
        sqlBuffer.append(TDConfig.insertStr).append(" into");
        int batchSize = 0;

        for (int l = readPos; l < writePos; ++l) {
            TDCsvLine csvLine = lines[l % maxLines];
            sqlBuffer.append(' ').append(TDConfig.datadbName).append('.').append(TDConfig.tablePrefix).append(csvLine.tableName)
                    .append(" values(").append(csvLine.timestamp);
            for (int i = 1; i < TDConfig.fields.length; ++i) {
                TDField field = TDConfig.fields[i];
                sqlBuffer.append(',');
                if (field.isTypeBinary && !TDConfig.binaryContainQuotation) {
                    sqlBuffer.append('\'');
                }
                for (int col : field.columns) {
                    String colStr = csvLine.cols[col];
                    if (colStr == null) {
                        sqlBuffer.append("NULL");
                    } else if (colStr.length() == 0) {
                        sqlBuffer.append("NULL");
                    } else {
                        sqlBuffer.append(colStr);
                    }
                }
                if (field.isTypeBinary && !TDConfig.binaryContainQuotation) {
                    sqlBuffer.append('\'');
                }
            }
            sqlBuffer.append(')');
            batchSize++;

            if (batchSize >= TDConfig.batchSize) {
                break;
            }
            if (sqlBuffer.toString().length() > 60000) {
                break;
            }
        }

        String processingSql = sqlBuffer.toString();
        int affectRows = executeInsertSql(processingSql);

        if (affectRows == batchSize) {
            insertedRows += affectRows;
        } else {
            if (TDConfig.retryOnError) {
                TDLog.print(String.format("file:%s, thread:%d inserted:%d batch:%d in [%d, %d) but affectRows:%d, need insert one by one"
                        , csv.getFileName(), threadIndex, insertedRows, batchSize, readPos, readPos+batchSize, affectRows));

                for (long i = readPos; i < readPos+batchSize; ++i) {
                    TDCsvLine line = lines[(int)(i % maxLines)];
                    String selectSql = line.getSelectSql();
                    String insertSql = line.getInsertSql();
                    int count = connection.executeQueryCount(selectSql);
                    if (count == 1) {
                        insertedRows++;
                        TDLog.trace(String.format("file:%s, thread:%d inserted:%d pos:%d line:%d already exist, sql:%s"
                                , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                    } else {
                        TDLog.trace(String.format("file:%s, thread:%d inserted:%d pos:%d line:%d not exist, sql:%s"
                                , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, selectSql));
                        affectRows = executeInsertSql(insertSql);
                        if (affectRows == 1) {
                            insertedRows++;
                            TDLog.trace(String.format("file:%s, thread:%d inserted:%d pos:%d line:%d not exist, insert success, affectRows:%d sql:%s"
                                    , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                        } else {
                            TDLog.error(String.format("file:%s, thread:%d inserted:%d pos:%d line:%d not exist, insert failed,affectRows:%d sql:%s"
                                    , csv.getFileName(), threadIndex, insertedRows, i, line.lineIndex, affectRows, insertSql));
                            TDLogDb.recordLine(threadIndex, csv.getFileName()
                                    , line.tableName, line.lineIndex, line.timestamp, connection.getErrorCode());
                        }
                    }
                } // end for
            }
            else {
                insertedRows += affectRows;
            }
        }

        readPos += batchSize;
    }

    private int executeInsertSql(String sql) {
        if (connection.executeUpdate(sql)) {
            return connection.getAffectrows();
        } else {
            return 0;
        }
    }
}
