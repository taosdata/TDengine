package com.taosdata.tsync.service;

import com.google.common.collect.Range;
import com.taosdata.tsync.entity.ProducerRecord;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.utils.DataGenerator;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ProduceToTQueueCallableTask implements Callable<Long> {
    private static final Logger logger = LoggerFactory.getLogger(ProduceToTQueueCallableTask.class);

    private int[] partitionsToWrite;
    private Range<Long> tablesToWrite;
    private long recordsToWrite;
    private long batchTables;
    private long batchValues;

    // schema
    private String dbname;
    private AtomicLong ts;

    private String stableName;
    private List<Configuration> columns;
    private List<Configuration> tags;

    private TQueueProducer<String> producer;
    private String topic;

    @Override
    public Long call() {
        logger.info(Thread.currentThread().getName() + " write table: " + tablesToWrite + " to partitions: " + Arrays.toString(partitionsToWrite) + " with records: " + recordsToWrite + ", batch values: " + batchValues + ", batch tables: " + batchTables);

        long count = 0;

        Map<Integer, OnePartitionTask> partitionIndexPerTask = divideTablesRecordsToEachPartition();

        for (int partitionId : partitionIndexPerTask.keySet()) {
            // TODO: error when partition = [1..10], thread = 1, tables = 1
            OnePartitionTask onePartitionTask = partitionIndexPerTask.get(partitionId);

            long[] tableIndexArr = LongStream.range(onePartitionTask.tableStartIndex, onePartitionTask.tableEndIndex).toArray();

            Map<Long, Range<Long>> tableIndex2RecordRange = Utils.divideIntoArrGroups(onePartitionTask.recordsToWrite, tableIndexArr);

            Map<Long, Range<Long>> batchIndex2TableRange = Utils.divideIntoGroupsOfN(onePartitionTask.tableStartIndex, onePartitionTask.tableEndIndex, batchTables);

            for (long tableBatchIndex : batchIndex2TableRange.keySet()) {
                long tableStartIndex = batchIndex2TableRange.get(tableBatchIndex).lowerEndpoint();
                long tableEndIndex = batchIndex2TableRange.get(tableBatchIndex).upperEndpoint();
                long recordsToWrite = sum(tableStartIndex, tableEndIndex, tableIndex2RecordRange);
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
                    if (message.length() > 16000) {
                        logger.error("message is too long");
                        continue;
                    }
                    logger.trace(message);
                    ProducerRecord<String> record = new ProducerRecord<>(topic, partitionId, message);
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

    private Map<Integer, OnePartitionTask> divideTablesRecordsToEachPartition() {
        Map<Long, Long> tableIndex2Records = Utils.divideIntoGroups(recordsToWrite, tablesToWrite);

        Map<Integer, OnePartitionTask> partitionTaskMap = new HashMap<>();
        // divide tables to each partitions
        Map<Integer, Range<Long>> partitionIndex2tableRange = Utils.divideRangeIntoArrayGroups(tablesToWrite, partitionsToWrite);

        for (int partitionIndex : partitionIndex2tableRange.keySet()) {
            Range<Long> tableRange = partitionIndex2tableRange.get(partitionIndex);
            long sum = 0;
            for (long tableIndex = tableRange.lowerEndpoint(); tableIndex < tableRange.upperEndpoint(); tableIndex++) {
                sum += tableIndex2Records.get(tableIndex);
            }
            OnePartitionTask task = new OnePartitionTask(tableRange.lowerEndpoint(), tableRange.upperEndpoint(), sum);
            partitionTaskMap.put(partitionIndex, task);
        }

        return partitionTaskMap;
    }

    public void shutdown() {
        producer.close();
    }

    private static class OnePartitionTask {
        private final long tableStartIndex;
        private final long tableEndIndex;
        private final long recordsToWrite;

        private OnePartitionTask(long tableStartIndex, long tableEndIndex, long recordsToWrite) {
            this.tableStartIndex = tableStartIndex;
            this.tableEndIndex = tableEndIndex;
            this.recordsToWrite = recordsToWrite;
        }
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
                if (j == 0 && column.getType().equalsIgnoreCase("timestamp")) {
                    parameters.add(ts.getAndIncrement());
                } else {
                    parameters.add(DataGenerator.random(column.getType(), column.getLength()));
                }
            }
        }
        return parameters.toArray();
    }

    // setters
    public void setPartitionsToWrite(int[] partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTablesToWrite(Range<Long> tablesToWrite) {
        this.tablesToWrite = tablesToWrite;
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

    public void setTs(AtomicLong ts) {
        this.ts = ts;
    }
}
