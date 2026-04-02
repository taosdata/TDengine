package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.enums.TmqMessageType;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_TMQ_CONSUMER_NULL;

public class JNIConsumer<V> implements  Consumer<V> {

    private final TMQConnector connector;

    public JNIConsumer() {
        connector = new TMQConnector();
    }

    @Override
    public void create(Properties properties) throws SQLException {
        String servers = properties.getProperty(TMQConstants.BOOTSTRAP_SERVERS);
        if (!StringUtils.isEmpty(servers)) {
            int lastColonIndex = servers.lastIndexOf(":");
            if (lastColonIndex == -1 || lastColonIndex == servers.length() - 1) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid bootstrap.servers format: " + servers);
            }

            String hostPart = servers.substring(0, lastColonIndex);
            String port = servers.substring(lastColonIndex + 1);
            properties.setProperty(TMQConstants.CONNECT_IP, hostPart);
            properties.setProperty(TMQConstants.CONNECT_PORT, port);
        }

        String host = properties.getProperty(TMQConstants.CONNECT_IP);
        if (host != null && host.startsWith("[") && host.endsWith("]")) {
            // IPv6 address
            host = host.substring(1, host.length() - 1);
            properties.setProperty(TMQConstants.CONNECT_IP, host);
        }

        long config = connector.createConfig(properties);
        try {
            connector.createConsumer(config);
        } finally {
            connector.destroyConf(config);
        }
    }

    @Override
    public void subscribe(Collection<String> topics) throws SQLException {
        long topicPointer = 0L;
        try {
            topicPointer = connector.createTopic(topics);
            connector.subscribe(topicPointer);
        } finally {
            if (topicPointer != TSDBConstants.JNI_NULL_POINTER) {
                connector.destroyTopic(topicPointer);
            }
        }
    }

    @Override
    public void unsubscribe() throws SQLException {
        connector.unsubscribe();
    }

    @Override
    public Set<String> subscription() throws SQLException {
        return connector.subscription();
    }

    @Override
    public ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException {
        long resultSet = connector.poll(timeout.toMillis());
        // when tmq pointer is null or result set is null
        if (resultSet == 0 || resultSet == ERROR_TMQ_CONSUMER_NULL) {
            return ConsumerRecords.emptyRecord();
        }

        int timestampPrecision = connector.getResultTimePrecision(resultSet);

        ConsumerRecords<V> records = new ConsumerRecords<>();
        String topic = connector.getTopicName(resultSet);
        String dbName = connector.getDbName(resultSet);
        int vGroupId = connector.getVgroupId(resultSet);
        long offset = connector.getOffset(resultSet);
        String tableName = connector.getTableName(resultSet);

        TopicPartition tp = new TopicPartition(topic, vGroupId);

        try (TMQResultSet rs = new TMQResultSet(connector, resultSet, timestampPrecision, dbName, tableName)) {
            while (rs.next()) {
                V v = deserializer.deserialize(rs, topic, dbName);
                ConsumerRecord<V> r = new ConsumerRecord.Builder<V>()
                        .topic(topic)
                        .dbName(dbName)
                        .vGroupId(vGroupId)
                        .offset(offset)
                        .messageType(TmqMessageType.TMQ_RES_DATA)
                        .value(v)
                        .build();
                records.put(tp, r);
            }
        }

        return records;
    }

    @Override
    public void commitAsync(OffsetCommitCallback<V> callback) throws SQLException {
        OffsetWaitCallback<V> call = new OffsetWaitCallback<>(getAllConsumed(), this, callback);

        connector.asyncCommit(call);
    }

    private Map<TopicPartition, OffsetAndMetadata> getAllConsumed() throws SQLException {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        this.subscription().forEach(topic -> {
            List<Assignment> topicAssignment = connector.getTopicAssignment(topic);
            topicAssignment.forEach(assignment -> {
                TopicPartition tp = new TopicPartition(topic, assignment.getVgId());
                OffsetAndMetadata metadata = new OffsetAndMetadata(assignment.getCurrentOffset());
                offsets.put(tp, metadata);
            });
        });
        return offsets;
    }

    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback<V> callback) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
            offset.put(entry.getKey(), entry.getValue());
            connector.asyncCommit(entry.getKey().getTopic(), entry.getKey().getVGroupId(), entry.getValue().offset(),
                    new OffsetWaitCallback<>(offset, this, callback));
        }
    }

    @Override
    public void commitSync() throws SQLException {
        connector.syncCommit();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) throws SQLException {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            connector.commitOffsetSync(entry.getKey().getTopic(), entry.getKey().getVGroupId(), entry.getValue().offset());
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        connector.seek(partition.getTopic(), partition.getVGroupId(), offset);
    }

    @Override
    public long position(TopicPartition partition) throws SQLException {
        return connector.position(partition.getTopic(), partition.getVGroupId());
    }

    @Override
    public Map<TopicPartition, Long> position(String topic) throws SQLException {
        List<TopicPartition> collect = connector.getTopicAssignment(topic).stream()
                .map(a -> new TopicPartition(topic, a.getVgId())).collect(Collectors.toList());
        Map<TopicPartition, Long> map = new HashMap<>();
        for (TopicPartition topicPartition : collect) {
            map.put(topicPartition, position(topicPartition));
        }
        return map;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(String topic) {
        return connector.getTopicAssignment(topic).stream()
                .collect(HashMap::new
                        , (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getBegin())
                        , HashMap::putAll
                );
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(String topic) {
        return connector.getTopicAssignment(topic).stream()
                .collect(HashMap::new
                        , (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getEnd())
                        , HashMap::putAll
                );
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException {
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
            if (beginningOffsets.containsKey(partition)) {
                Long aLong = beginningOffsets.get(partition);
                seek(partition, aLong);
            } else {
                beginningOffsets(partition.getTopic()).forEach((tp, offset) -> {
                    if (tp.getVGroupId() == partition.getVGroupId()) {
                        seek(tp, offset);
                    } else {
                        beginningOffsets.put(tp, offset);
                    }
                });
            }
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) throws SQLException {
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
            if (endOffsets.containsKey(partition)) {
                Long aLong = endOffsets.get(partition);
                seek(partition, aLong);
            } else {
                endOffsets(partition.getTopic()).forEach((tp, offset) -> {
                    if (tp.getVGroupId() == partition.getVGroupId()) {
                        seek(tp, offset);
                    } else {
                        endOffsets.put(tp, offset);
                    }
                });
            }
        }
    }

    @Override
    public Set<TopicPartition> assignment() throws SQLException {
        return subscription().stream().map(topic -> {
            List<Assignment> topicAssignment = connector.getTopicAssignment(topic);
            return topicAssignment.stream().map(a -> new TopicPartition(topic, a.getVgId())).collect(Collectors.toList());
        }).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) throws SQLException {
        long l = connector.committed(partition.getTopic(), partition.getVGroupId());
        return new OffsetAndMetadata(l, null);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) throws SQLException {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        for (TopicPartition partition : partitions) {
            map.put(partition, committed(partition));
        }
        return map;
    }

    @Override
    public void close() throws SQLException {
        connector.closeConsumer();
    }

    public String getErrMsg(int code) {
        return connector.getErrMsg(code);
    }
}
