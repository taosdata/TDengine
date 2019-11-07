package com.taosdata.hdfs.csv;

import com.taosdata.hdfs.csv.internal.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TDCsvCache extends TDCsv {
    public TDCsvCache(String fileName) {
        this.fileName = fileName;
        this.fileBaseName = TDUtil.getBaseFileName(this.fileName);
    }

    public boolean parseStream(InputStreamReader infile) {
        BufferedReader br = new BufferedReader(infile);
        try {
            TDLog.print(String.format("file:%s, open success", this.fileName));

            String line = null;
            lineIndex = 0;
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

                parsedRows++;
                lines.add(csvLine);

                if (lines.size() >= TDConfig.fileCacheRows) {
                    parseTimeTs += (float) (TDUtil.getTimeStampMs() - startTs) / 1000;
                    TDLog.print(String.format("file:%s, writePartition:%d totalReadLines:%d, totalParsedLines:%d ", this.fileName, writePartition, lineIndex, parsedRows));
                    importToTDengine();
                    lines.clear();
                    startTs = TDUtil.getTimeStampMs();
                    writePartition++;
                }
            }

            if (lines.size() != 0) {
                parseTimeTs += (float) (TDUtil.getTimeStampMs() - startTs) / 1000;
                TDLog.print(String.format("file:%s, writePartition:%d totalReadLines:%d, totalParsedLines:%d", this.fileName, writePartition, lineIndex, parsedRows));
                importToTDengine();
                lines.clear();
                startTs = TDUtil.getTimeStampMs();
                writePartition++;
            }

            TDLog.print(String.format("file:%s, handle successfully, totalReadLines:%d, totalParsedLines:%d, totalInsertLines:%d"
                    , this.fileName, lineIndex, parsedRows, insertedRows));
            TDLogDb.recordFile(this);

        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("file:%s, processing failed, error:%s", this.fileName, e.getMessage()));
            return false;
        } finally {
            try {
                br.close();
                infile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    private void importToTDengine() {
        if (TDConfig.sortBeforeInsert) {
            Collections.sort(lines, new Comparator<TDCsvLine>() {
                @Override
                public int compare(TDCsvLine line1, TDCsvLine line2) {
                    long cmp = line1.tableName.compareToIgnoreCase(line2.tableName);
                    if (cmp == 0) {
                        cmp = line1.timestamp - line2.timestamp;
                    }

                    if (cmp > 0) {
                        return 1;
                    } else if (cmp < 0) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });
            TDLog.print(String.format("file:%s, writePartition:%d sort success", fileName, writePartition));
        }

        if (TDConfig.retainDuplicate && !TDConfig.autoTimestamp) {
            long lastTs = lines.get(0).timestamp;
            String lastTb = lines.get(0).tableName;
            int repeat = 0;
            for (int i = 1; i < lines.size(); ++i) {
                TDCsvLine tmp = lines.get(i);
                if (tmp.timestamp != lastTs) {
                    lastTs = tmp.timestamp;
                    lastTb = tmp.tableName;
                    repeat = 0;
                    continue;
                }
                if (!tmp.tableName.equalsIgnoreCase(lastTb)) {
                    lastTs = tmp.timestamp;
                    lastTb = tmp.tableName;
                    repeat = 0;
                    continue;
                }
                tmp.timestamp += (++repeat);
                TDLog.trace(String.format("file:%s, line:%d pos:%d table:%s ts:%d changed to %d", fileName, tmp.lineIndex, i, tmp.tableName, lastTs, tmp.timestamp));
            }
        }

        TDDataDb.createTb(this);
        TDCsvCacheThreads threads = new TDCsvCacheThreads();
        threads.init(this);
        threads.run();
    }

    public Object[] getProcessingSql(int start, int maxEnd) {
        StringBuilder sqlBuffer = new StringBuilder();
        sqlBuffer.append(TDConfig.insertStr).append(" into");

        int totalLen = 11;
        int batchSize = 0;

        for (int l = start; l < maxEnd; ++l) {
            TDCsvLine csvLine = lines.get(l);
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

        Object ret[] = new Object[2];
        ret[0] = sqlBuffer.toString();
        ret[1] = start + batchSize;

        return ret;
    }

    public int getLineEnd(int lineBegin, int part, int total) {
        if (part == total) {
            return lines.size();
        }

        float ratio = (float) part / (float) total;
        if (ratio > 1) ratio = 1;

        int lineEnd = (int) ((float) lines.size() * ratio) - 1;
        if (lineEnd < 0) lineEnd = 0;

        String lastTbName = lines.get(lineEnd).tableName;
        if (lastTbName != null) {
            for (int i = lineEnd + 1; i < (int) lines.size(); ++i) {
                if (!lastTbName.equalsIgnoreCase(lines.get(i).tableName)) {
                    return i;
                }
            }
        }

        return (int) lines.size();
    }

    private int writePartition = 1;
    public ArrayList<TDCsvLine> lines = new ArrayList<TDCsvLine>();
}

class TDCsvCacheThreads {
    public TDCsvCache csv;
    private long startTs;
    private long endTs;
    private ArrayList<TDCsvCacheThread> threads = new ArrayList<TDCsvCacheThread>();

    public void init(TDCsvCache csv) {
        this.csv = csv;
        TDLog.print(String.format("file:%s, init %d threads...", csv.getFileName(), TDConfig.csvThreadNum));

        this.startTs = TDUtil.getTimeStampMs();
        for (int threadIndex = 0; threadIndex < TDConfig.csvThreadNum; ++threadIndex) {
            TDCsvCacheThread thread = new TDCsvCacheThread();
            this.threads.add(thread);
            int lineBegin, lineEnd;
            if (threadIndex == 0) {
                lineBegin = 0;
                lineEnd = csv.getLineEnd(lineBegin, threadIndex + 1, TDConfig.csvThreadNum);
            } else {
                lineBegin = threads.get(threadIndex - 1).getLineEnd();
                lineEnd = csv.getLineEnd(lineBegin, threadIndex + 1, TDConfig.csvThreadNum);
            }
            thread.init(csv, lineBegin, lineEnd, threadIndex);
        }
    }

    public void run() {
        TDLog.print(String.format("file:%s, threads start to run", csv.getFileName()));

        ExecutorService executorService = Executors.newFixedThreadPool(this.threads.size());
        for (TDCsvCacheThread thread : this.threads) {
            executorService.execute(thread);
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }

        this.endTs = TDUtil.getTimeStampMs();
        float insertedSec = (float) (this.endTs - this.startTs) / 1000;
        int insertedRows = 0;
        for (TDCsvCacheThread thread : this.threads) {
            TDLog.print(String.format("thread:%d insert:%d", thread.getThreadIndex(), thread.getInsertedRows()));
            insertedRows += thread.getInsertedRows();
        }
        csv.addInsertRows(insertedRows);
        csv.addInsertTimeSec(insertedSec);

        TDLog.print(String.format("file:%s, threads stop successfully, insert:%d, totalInserted:%d", csv.getFileName(), insertedRows, csv.getInsertedRows()));
    }
}

class TDCsvCacheThread implements Runnable {
    private TDCsvCache csv;
    private int threadIndex;
    private int lineBegin;
    private int lineEnd;
    private int insertedRows;
    private TDConnection connection = null;

    public int getLineEnd() {
        return this.lineEnd;
    }

    public int getLineBegin() {
        return this.lineBegin;
    }

    public int getThreadIndex() {
        return this.threadIndex;
    }

    public int getInsertedRows() {
        return this.insertedRows;
    }

    public void init(TDCsvCache csv, int lineBegin, int lineEnd, int threadIndex) {
        this.threadIndex = threadIndex;
        this.csv = csv;
        this.lineBegin = lineBegin;
        this.lineEnd = lineEnd;
        this.connection = TDDataDb.getConnection(threadIndex);
        if (lineEnd < lineBegin) {
            lineEnd = lineBegin;
        }

        TDLog.print(String.format("file:%s, thread:%d init, lineBegin:%d, lineEnd:%d", csv.getFileName(), threadIndex, lineBegin, lineEnd));
    }

    public void run() {
        if (lineEnd <= lineBegin) {
            return;
        }
        TDLog.print(String.format("file:%s, thread:%d is running", csv.getFileName(), threadIndex));

        insertedRows = 0;
        int processingBegin = lineBegin;
        while (processingBegin < lineEnd) {
            Object ret[] = csv.getProcessingSql(processingBegin, lineEnd);
            String processingSql = (String) ret[0];
            int processingEnd = (Integer) ret[1];
            int processingRows = processingEnd - processingBegin;
            int affectRows = executeInsertSql(processingSql);

            if (affectRows == processingRows) {
                insertedRows += affectRows;
            } else {
                if (TDConfig.retryOnError) {
                    TDLog.print(String.format("file:%s, thread:%d inserted:%d rows:%d in [%d, %d) but affectRows:%d, need insert one by one"
                            , csv.getFileName(), threadIndex, insertedRows, processingRows, processingBegin, processingEnd, affectRows));

                    for (int i = processingBegin; i < processingEnd; ++i) {
                        TDCsvLine line = csv.lines.get(i);
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

            processingBegin = processingEnd;
        }

        TDLog.print(String.format("file:%s, thread:%d process successfully, expectInsertRows:%d, insertedRows:%d"
                , csv.getFileName(), threadIndex, lineEnd - lineBegin, insertedRows));
    }

    private int executeInsertSql(String sql) {
        if (connection.executeUpdate(sql)) {
            return connection.getAffectrows();
        } else {
            return 0;
        }
    }
}
