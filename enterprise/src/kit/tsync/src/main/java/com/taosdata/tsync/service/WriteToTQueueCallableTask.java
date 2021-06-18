package com.taosdata.tsync.service;

import com.google.common.collect.Range;
import com.taosdata.tsync.utils.DataGenerator;
import com.taosdata.tsync.TQueueProducer;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.entity.producer.ProducerRecord;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class WriteToTQueueCallableTask implements Callable<Long> {
    private static final Logger logger = LoggerFactory.getLogger(WriteToTQueueCallableTask.class);

    private List<Integer> partitionsToWrite;
    private Range<Long> tablesToWrite;

    private long recordsToWrite;
    private long batchTables;
    private long batchValues;

    private String dbname;
    private String stableName;
    private List<Configuration> columns;
    private List<Configuration> tags;

    private String topic;
    private TQueueProducer producer;

    private volatile AtomicLong ts;

    private class OnePartitionTask {
        private final long tableStartIndex;
        private final long tableEndIndex;
        private final long tableTotal;
        private final int partitionId;
        private final long recordsToWrite;

        private OnePartitionTask(long tableStartIndex, long tableEndIndex, long tableTotal, int partitionId, long recordsToWrite, long batchValues, long batchTables) {
            this.tableStartIndex = tableStartIndex;
            this.tableEndIndex = tableEndIndex;
            this.tableTotal = tableTotal;
            this.partitionId = partitionId;
            this.recordsToWrite = recordsToWrite;
        }
    }

    @Override
    public Long call() {
        long count = 0;

        Map<Integer, OnePartitionTask> partitionIndexPerTask = divideTablesRecordsToEachPartition();
        for (int partitionId : partitionIndexPerTask.keySet()) {
            OnePartitionTask onePartitionTask = partitionIndexPerTask.get(partitionId);
            long[] tableIndexArr = LongStream.range(onePartitionTask.tableStartIndex, onePartitionTask.tableEndIndex).toArray();
            Map<Long, Range<Long>> tableIndex2RecordRange = Utils.divideIntoArrGroups(onePartitionTask.recordsToWrite, tableIndexArr);
            Map<Long, Range<Long>> batchIndex2TableRange = Utils.divideIntoGroupsOfN(onePartitionTask.tableStartIndex, onePartitionTask.tableEndIndex, batchTables);

            for (long tableBatchIndex : batchIndex2TableRange.keySet()) {
                long tableStartIndex = batchIndex2TableRange.get(tableBatchIndex).lowerEndpoint();
                long tableEndIndex = batchIndex2TableRange.get(tableBatchIndex).upperEndpoint();
                long recordsToWrite = sum(tableStartIndex, tableEndIndex, tableIndex2RecordRange);

                logger.info(Thread.currentThread().getName() + " write table: [" + tableStartIndex + "," + tableEndIndex + ")" + " to partitions: " + partitionId + " with records: " + recordsToWrite + " batch values: " + batchValues + " batch tables: " + batchTables);

                long valueCount = 0;
                while (valueCount < recordsToWrite) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("insert into");
                    for (long tableIndex = tableStartIndex; tableIndex < tableEndIndex; tableIndex++) {
                        long values;
                        if (valueCount == recordsToWrite) {
                            continue;
                        } else if (valueCount + batchValues <= recordsToWrite) {
                            values = batchValues;
                        } else {
                            values = recordsToWrite - valueCount;
                        }

                        String preparedSql = preparedSqlPerTable(columns.size(), tags.size(), values);
                        Object[] parameters = preparedParametersPerTable(tableIndex, values);
                        String sqlPerTable = com.taosdata.jdbc.utils.Utils.getNativeSql(preparedSql, parameters);
                        sb.append(sqlPerTable);
                        valueCount += values;
                    }

                    String message = sb.toString();
                    logger.trace(message);
                    ProducerRecord<String> record = new ProducerRecord(topic, partitionId, message);
                    try {
                        producer.send(record);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                count += valueCount;
            }

        }

        return count;
    }

    /**
     * calculate sum of map from startIndex(include) to endIndex(exclude)
     */
    private long sum(long startIndex, long endIndex, Map<Long, Range<Long>> map) {
        long sum = 0;
        for (long index = startIndex; index < endIndex; index++) {
            sum += map.get(index).upperEndpoint() - map.get(index).lowerEndpoint();
        }
        return sum;
    }

    private Map<Integer, OnePartitionTask> divideTablesRecordsToEachPartition() {
        int[] partitionsToWriteArr = partitionsToWrite.stream().mapToInt(i -> i.intValue()).toArray();
        Map<Integer, OnePartitionTask> partitionTaskMap = new HashMap<>();

        Map<Integer, Range<Long>> partitionIndex2TableRange = Utils.divideIntoArrGroups(tablesToWrite.lowerEndpoint(), tablesToWrite.upperEndpoint(), partitionsToWriteArr);
        Map<Integer, Range<Long>> partitionIndex2RecordRange = Utils.divideIntoArrGroups(recordsToWrite, partitionsToWriteArr);
        for (int partitionId : partitionIndex2TableRange.keySet()) {
            Range<Long> tablesRangeToWrite = partitionIndex2TableRange.get(partitionId);
            Range<Long> recordRange = partitionIndex2RecordRange.get(partitionId);

            long tableStartIndex = tablesRangeToWrite.lowerEndpoint();
            long tableEndIndex = tablesRangeToWrite.upperEndpoint();
            long tableTotal = tableEndIndex - tableStartIndex;
            long recordsToWrite = recordRange.upperEndpoint() - recordRange.lowerEndpoint();

            OnePartitionTask onePartitionTask = new OnePartitionTask(tableStartIndex, tableEndIndex, tableTotal, partitionId, recordsToWrite, batchValues, batchTables);
            partitionTaskMap.put(partitionId, onePartitionTask);
        }
        return partitionTaskMap;
    }

    // ?.? using ?.? tags (?,?,?) values(?,?,?),(?,?,?)
    private String preparedSqlPerTable(int columnSize, int tagSize, long records) {
        String columnsMark = IntStream.range(0, columnSize).mapToObj(i -> "?").collect(Collectors.joining(",", "(", ")"));
        String tagsMark = IntStream.range(0, tagSize).mapToObj(i -> "?").collect(Collectors.joining(",", "(", ")"));

        StringBuilder sb = new StringBuilder();
        sb.append(" ?.?");
        sb.append(columnsMark);
        sb.append(" using ?.? tags ");
        sb.append(tagsMark);
        sb.append(" values");
        String valuesMark = LongStream.range(0, records).mapToObj(i -> columnsMark).collect(Collectors.joining(",", "", " "));
        sb.append(valuesMark);
        return sb.toString();
    }

    private Object[] preparedParametersPerTable(long tableInd, long records) {
        List<Object> parameters = new ArrayList<>();
        // sql
        String tablename = "t" + tableInd;
        // set parameters
        parameters.add(dbname);
        parameters.add(tablename);
        for (int i = 0; i < columns.size(); i++) {
            ColumnConfiguration column = (ColumnConfiguration) columns.get(i);
            parameters.add(column.getName());
        }
        parameters.add(dbname);
        parameters.add(stableName);
        for (int i = 0; i < tags.size(); i++) {
            TagConfiguration tag = (TagConfiguration) tags.get(i);
            parameters.add(DataGenerator.random(tag.getType(), tag.getLength()));
        }
        for (long i = 0; i < records; i++) {
            for (int j = 0; j < columns.size(); j++) {
                ColumnConfiguration column = (ColumnConfiguration) columns.get(j);
                parameters.add(DataGenerator.random(column.getType(), column.getLength()));
            }
        }
        return parameters.toArray();
    }

    // setters
    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTablesToWrite(Range<Long> tablesToWrite) {
        this.tablesToWrite = tablesToWrite;
    }

    public void setTables(long tables) {
    }

    public void setRecordsToWrite(long recordsToWrite) {
        this.recordsToWrite = recordsToWrite;
    }

    public void setBatchTables(long batchTables) {
        this.batchTables = batchTables;
    }

    public void setBatchValues(long batchValues) {
        this.batchValues = batchValues;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public void setStableName(String stableName) {
        this.stableName = stableName;
    }

    public void setColumns(List<Configuration> columns) {
        this.columns = columns;
    }

    public void setTags(List<Configuration> tags) {
        this.tags = tags;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setProducer(TQueueProducer producer) {
        this.producer = producer;
    }

    public void setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
    }
}
