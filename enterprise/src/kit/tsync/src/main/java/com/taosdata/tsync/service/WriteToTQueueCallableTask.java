package com.taosdata.tsync.service;

import com.google.common.collect.Range;
import com.taosdata.taosdemo.utils.DataGenerator;
import com.taosdata.tsync.TQueueProducer;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.entity.producer.ProducerRecord;
import com.taosdata.tsync.utils.SqlSyntaxUtil;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class WriteToTQueueCallableTask implements Callable {
    private static final Logger logger = LoggerFactory.getLogger(WriteToTQueueCallableTask.class);

    private Collection<Integer> partitionsToWrite;
    private Range<Long> tablesToWrite;
    private long tables;

    private long recordsToWrite;
    private long batchTables;
    private long batchValues;

    private String dbname;
    private String stableName;
    private List<Configuration> columns;
    private List<Configuration> tags;

    private String topic;
    private TQueueProducer producer;
    private SchemaConfiguration schemaConfiguration;

    @Override
    public Integer call() throws Exception {
        logger.info(Thread.currentThread().getName() + " write table: " + tablesToWrite + " to partitions: " + Arrays.toString(partitionsToWrite.toArray())
                + " with records: " + recordsToWrite + ", batch values: " + batchValues + ", batch tables: " + batchTables);

        int count = 0;


        return count;

        /*
        // divide total records to each table
        Map<Long, Range<Long>> tableRecords = Utils.divideIntoGroups(recordsToWrite, tables);
        Map<Long, Long> tableIndex2Records = new HashMap<>();
        for (long tableIndex = tablesToWrite.lowerEndpoint(), index = 0; index < tables && tableIndex < tablesToWrite.upperEndpoint(); index++, tableIndex++) {
            long records = tableRecords.get(index).upperEndpoint() - tableRecords.get(index).lowerEndpoint();
            tableIndex2Records.put(tableIndex, records);
        }
        // divide each tables record to batch
        Map<Long, Map<Long, Long>> tableIndex2BatchRecords = new HashMap<>();
        for (long tableIndex = tablesToWrite.lowerEndpoint(); tableIndex < tablesToWrite.upperEndpoint(); tableIndex++) {
            long recordCount = tableIndex2Records.get(tableIndex);
            Map<Long, Long> batchIndex2Records = Utils.divideIntoGroupsOfN(recordCount, batchValues);
            tableIndex2BatchRecords.put(tableIndex, batchIndex2Records);
        }

        Map<Long, Range<Long>> batchIndex2TableRange = Utils.divideIntoGroupsOfN(tablesToWrite.lowerEndpoint(), tablesToWrite.upperEndpoint(), this.batchTables);
        for (long tableBatchIndex = 0; tableBatchIndex < batchIndex2TableRange.size(); tableBatchIndex++) {

            long startTableIndex = batchIndex2TableRange.get(tableBatchIndex).lowerEndpoint();
            long endTableIndex = batchIndex2TableRange.get(tableBatchIndex).upperEndpoint();
            logger.trace(">>> " + Thread.currentThread().getName() + ", tableRange: [" + startTableIndex + "..." + endTableIndex + ") <<<");

            // each table batch
            for (long recordBatchIndex = 0; ; recordBatchIndex++) {
                boolean hasRecords = hasNoRecordsForThisRecordBatchIndex(recordBatchIndex, tableIndex2BatchRecords, startTableIndex, endTableIndex);
                if (!hasRecords) {
                    break;
                }
                // message
                StringBuilder message = new StringBuilder();
                message.append("insert into");
                for (long tableIndex = startTableIndex; tableIndex < endTableIndex; tableIndex++) {
                    if (tableIndex2BatchRecords.get(tableIndex).containsKey(recordBatchIndex)) {
                        Long records = tableIndex2BatchRecords.get(tableIndex).get(recordBatchIndex);
                        String sql = createPreparedSql(columns.size(), tags.size(), records);
                        Object[] parameters = createParameters(dbname, tableIndex, stableName, columns, tags, records);
                        message.append(SqlSyntaxUtil.getNativeSql(sql, parameters));
                        count += records;
                        logger.trace("recordBatchIndex: " + recordBatchIndex + ", tableIndex: " + tableIndex + ", records: " + records);
                    }
                }
                // partitionId
                int[] partitionsToWriteArr = partitionsToWrite.stream().mapToInt(i -> i.intValue()).toArray();
                int partitionId = calculatePartitionId(tableBatchIndex, partitionsToWriteArr);
                // record
                ProducerRecord<String> record = new ProducerRecord<>(topic, partitionId, message.toString());
                logger.trace("topic: " + record.getTopic() + ", partitionId: " + record.getPartition() + ", message: " + record.getMessage());
            }
        }
*/

    }

    private boolean hasNoRecordsForThisRecordBatchIndex(long batchIndex, Map<Long, Map<Long, Long>> tableIndex2BatchRecords, long startTableIndex, long endTableIndex) {
        for (long tableIndex = startTableIndex; tableIndex < endTableIndex; tableIndex++) {
            if (tableIndex2BatchRecords.get(tableIndex).containsKey(batchIndex)) {
                return true;
            }
        }
        return false;
    }

    private String createPreparedSql(int columnSize, int tagSize, long records) {
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

    private int calculatePartitionId(long batchIndex, int[] partitionsToWriteArr) {
        // partition
        int partitionIndex = (int) (batchIndex % partitionsToWriteArr.length);
        int partitionId = partitionsToWriteArr[partitionIndex];
        return partitionId;
    }

    private Object[] createParameters(String dbname, long tableInd, String stableName, List<Configuration> columns, List<Configuration> tags, long records) {
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
    public void setPartitionsToWrite(Collection<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTablesToWrite(Range<Long> tablesToWrite) {
        this.tablesToWrite = tablesToWrite;
    }

    public void setTables(long tables) {
        this.tables = tables;
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
        this.schemaConfiguration = schemaConfiguration;
    }
}
