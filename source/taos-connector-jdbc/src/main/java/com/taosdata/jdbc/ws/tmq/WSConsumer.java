package com.taosdata.jdbc.ws.tmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.enums.TmqMessageType;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.utils.VersionUtil;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.tmq.entity.*;
import com.taosdata.jdbc.ws.tmq.meta.Meta;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.TSDBConstants.MIN_SUPPORT_VERSION;

public class WSConsumer<V> implements Consumer<V> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WSConsumer.class);
    private Transport transport;
    private ConsumerParam param;
    private TMQRequestFactory factory;
    private long lastCommitTime = 0;
    private long messageId = 0L;
    private long lastMessageId = 0L;

    private Collection<String> topics;
    @Override
    public void create(Properties properties) throws SQLException {
        factory = new TMQRequestFactory();
        param = new ConsumerParam(properties);
        InFlightRequest inFlightRequest = new InFlightRequest(param.getConnectionParam().getMaxRequest());

        param.getConnectionParam().setTextMessageHandler(message -> {
            try {
                JsonNode jsonObject = JsonUtil.getObjectReader().readTree(message);
                ConsumerAction action = ConsumerAction.of(jsonObject.get("action").asText());
                ObjectReader actionReader = JsonUtil.getObjectReader(action.getResponseClazz());
                Response response = actionReader.treeToValue(jsonObject, action.getResponseClazz());
                FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
                if (null != remove) {
                    remove.getFuture().complete(response);
                }
            } catch (JsonProcessingException e) {
                log.error("Error processing message", e);
            }
        });

        param.getConnectionParam().setBinaryMessageHandler(byteBuf -> {
            byteBuf.order(ByteOrder.LITTLE_ENDIAN);
            byteBuf.readerIndex(26);
            long id = byteBuf.readLongLE();
            byteBuf.readerIndex(8);
            FutureResponse remove = inFlightRequest.remove(ConsumerAction.FETCH_RAW_DATA.getAction(), id);
            if (null != remove) {
                Utils.retainByteBuf(byteBuf);
                FetchRawBlockResp fetchBlockResp = new FetchRawBlockResp(byteBuf);
                remove.getFuture().complete(fetchBlockResp);
            }
        });

        transport = new Transport(WSFunction.TMQ, param.getConnectionParam(), inFlightRequest);
        transport.checkConnection(param.getConnectionParam().getConnectTimeout());
    }

    @Override
    public void subscribe(Collection<String> topics) throws SQLException {
        Request request = factory.generateSubscribe(param
                , topics.toArray(new String[0])
                , String.valueOf(false)
        );
        SubscribeResp response = (SubscribeResp) transport.send(request);
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("subscribe topic error, code: (0x" + Integer.toHexString(response.getCode())
                    + "), message: " + response.getMessage());
        }

        String version = response.getVersion();
        if (version == null) {
            version = VersionUtil.getVersion(transport);
        }
        if (!VersionUtil.checkVersion(version)) {
            transport.close();
            throw new SQLException("The version of the server is not supported, please upgrade to at least " + MIN_SUPPORT_VERSION);
        }

        this.topics = topics;
    }

    @Override
    public void unsubscribe() throws SQLException {
        Request request = factory.generateUnsubscribe();
        UnsubscribeResp response = (UnsubscribeResp) transport.send(request);
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("unsubscribe topic error, code: (0x" + Integer.toHexString(response.getCode())
                    + "), message: " + response.getMessage() + ", timing: " + response.getTiming());
        }
    }

    @Override
    public Set<String> subscription() throws SQLException {
        Request request = factory.generateSubscription();
        ListTopicsResp response = (ListTopicsResp) transport.send(request);
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("get subscription error, code: (0x" + Integer.toHexString(response.getCode())
                    + "), message: " + response.getMessage());
        }
        return Arrays.stream(response.getTopics()).collect(Collectors.toSet());
    }

    private boolean handleReconnect() throws SQLException {
        transport.reconnectTmq();
        subscribe(this.topics);
        return true;
    }

    private ConsumerRecords<V> getMeta(PollResp pollResp) throws SQLException{
        Request fetchJsonMetaReq = factory.generateFetchJsonMeata(pollResp.getMessageId());
        FetchJsonMetaResp fetchJsonMetaResp = (FetchJsonMetaResp) transport.send(fetchJsonMetaReq);
        if (Code.SUCCESS.getCode() != fetchJsonMetaResp.getCode()) {
            throw new SQLException("consumer fetch json meta error, code: (0x" + Integer.toHexString(fetchJsonMetaResp.getCode()) + "), message: " + fetchJsonMetaResp.getMessage());
        }

        if (fetchJsonMetaResp.getData() == null || fetchJsonMetaResp.getData().getMetas() == null) {
            return ConsumerRecords.emptyRecord();
        }

        ConsumerRecords<V> records = new ConsumerRecords<>();
        TopicPartition tp = new TopicPartition(pollResp.getTopic(), pollResp.getVgroupId());

        for (Meta meta : fetchJsonMetaResp.getData().getMetas()){
            ConsumerRecord<V> r = new ConsumerRecord.Builder<V>()
                    .topic(pollResp.getTopic())
                    .dbName(pollResp.getDatabase())
                    .vGroupId(pollResp.getVgroupId())
                    .offset(pollResp.getOffset())
                    .messageType(TmqMessageType.TMQ_RES_TABLE_META)
                    .meta(meta)
                    .value(null)
                    .build();
            records.put(tp, r);
        }
        return records;
    }

    private ConsumerRecords<V> getData(PollResp pollResp, Deserializer<V> deserializer) throws SQLException{
        ConsumerRecords<V> records = new ConsumerRecords<>();
        try (WSConsumerResultSet rs = new WSConsumerResultSet(transport, factory, pollResp.getMessageId(), pollResp.getDatabase(), param.getConnectionParam().getZoneId())) {
            if (deserializer instanceof MapEnhanceDeserializer){
                ConsumerRecords<TMQEnhMap> resultRecords = rs.handleSubscribeDB(pollResp);
                return (ConsumerRecords<V>) resultRecords;
            }
            String topic = pollResp.getTopic();
            String dbName = pollResp.getDatabase();
            int vGroupId = pollResp.getVgroupId();
            TopicPartition tp = new TopicPartition(topic, vGroupId);

            while (rs.next()) {
                V v = deserializer.deserialize(rs, topic, dbName);
                ConsumerRecord<V> r = new ConsumerRecord.Builder<V>()
                        .topic(topic)
                        .dbName(dbName)
                        .vGroupId(vGroupId)
                        .offset(pollResp.getOffset())
                        .messageType(TmqMessageType.TMQ_RES_DATA)
                        .meta(null)
                        .value(v)
                        .build();
                records.put(tp, r);
            }
        }
        return records;
    }

    private ConsumerRecords<V> doPoll(Duration timeout, Deserializer<V> deserializer) throws SQLException{
        if (param.isAutoCommit() && (0 != messageId)) {
            long now = System.currentTimeMillis();
            if (now - lastCommitTime > param.getAutoCommitInterval()) {
                commitSync();
                lastCommitTime = now;
            }
        }

        Request request = factory.generatePoll(lastMessageId, timeout.toMillis());
        PollResp pollResp = (PollResp) transport.send(request);

        if (Code.SUCCESS.getCode() != pollResp.getCode()) {
            throw new SQLException("consumer poll error, code: (0x" + Integer.toHexString(pollResp.getCode()) + "), message: " + pollResp.getMessage());
        }
        if (!pollResp.isHaveMessage()) {
            return ConsumerRecords.emptyRecord();
        }

        messageId = pollResp.getMessageId();
        lastMessageId = messageId;

        if (pollResp.getMessageType() == TmqMessageType.TMQ_RES_TABLE_META.getCode()){
            return getMeta(pollResp);
        }

        if (pollResp.getMessageType() == TmqMessageType.TMQ_RES_DATA.getCode()){
            return getData(pollResp, deserializer);
        }

        if (pollResp.getMessageType() == TmqMessageType.TMQ_RES_METADATA.getCode()) {
            ConsumerRecords<V> metaRecords = getMeta(pollResp);
            ConsumerRecords<V> dataRecords = getData(pollResp, deserializer);
            if (metaRecords.isEmpty()){
                return dataRecords;
            }
            if (dataRecords.isEmpty()){
                return metaRecords;
            }

            TopicPartition tp = new TopicPartition(pollResp.getTopic(), pollResp.getVgroupId());

            for (ConsumerRecord<V> r : metaRecords.get(tp)){
                dataRecords.put(tp, r);
            }

            return dataRecords;
        }
        return ConsumerRecords.emptyRecord();
    }

    @Override
    public ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException {

        try {
            return doPoll(timeout, deserializer);
        } catch (SQLException e) {
            if ((e.getErrorCode() == TSDBErrorNumbers.ERROR_CONNECTION_CLOSED
                    && !transport.isClosed()
                    && this.param.getConnectionParam().isEnableAutoConnect()
                    && handleReconnect())) {
                // when reconnect success, skip once auto commit for the message id is invalid
                messageId = 0;
                return ConsumerRecords.emptyRecord();
            }
            // time out due to connection lost
            if (e.getErrorCode() == TSDBErrorNumbers.ERROR_QUERY_TIMEOUT
                    && !transport.isClosed()
                    && transport.isConnectionLost()
                    && handleReconnect()) {
                messageId = 0;
                return ConsumerRecords.emptyRecord();
            }
            throw e;
        }
    }

    @Override
    public synchronized void commitSync() throws SQLException {
        if (0 != messageId) {
            CommitResp commitResp = (CommitResp) transport.send(factory.generateCommit(messageId));
            if (Code.SUCCESS.getCode() != commitResp.getCode()) {
                throw new SQLException("consumer commit error. code: (0x" + Integer.toHexString(commitResp.getCode()) + "), message: " + commitResp.getMessage());
            }
            messageId = 0;
        }
    }

    @Override
    public void close() throws SQLException {
        transport.close();
    }

    @Override
    public void commitAsync(OffsetCommitCallback<V> callback) {
        // nothing to do
    }

    @Override
    public void seek(TopicPartition partition, long offset) throws SQLException {
        Request request = factory.generateSeek(partition.getTopic(), partition.getVGroupId(), offset);
        SeekResp resp = (SeekResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer seek error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }
    }

    @Override
    public long position(TopicPartition partition) throws SQLException {
        Request request = factory.generatePosition(new TopicPartition[]{partition});
        PositionResp resp = (PositionResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer position error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }
        return resp.getPosition()[0];
    }

    @Override
    public Map<TopicPartition, Long> position(String topic) throws SQLException {
        TopicPartition[] topicPartitions = Arrays.stream(getAssignment(topic))
                .map(a -> new TopicPartition(topic, a.getVgId()))
                .toArray(TopicPartition[]::new);
        Request request = factory.generatePosition(topicPartitions);
        PositionResp resp = (PositionResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer position error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }

        return Arrays.stream(topicPartitions)
                .collect(Collectors.toMap(tp -> tp, tp -> resp.getPosition()[Arrays.asList(topicPartitions).indexOf(tp)]));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(String topic) throws SQLException {
        return Arrays.stream(getAssignment(topic))
                .collect(HashMap::new, (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getBegin()), HashMap::putAll);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(String topic) throws SQLException {
        return Arrays.stream(getAssignment(topic))
                .collect(HashMap::new, (m, a) -> m.put(new TopicPartition(topic, a.getVgId()), a.getEnd()), HashMap::putAll);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException {
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
            if (beginningOffsets.containsKey(partition)) {
                Long aLong = beginningOffsets.get(partition);
                seek(partition, aLong);
            } else {
                Map<TopicPartition, Long> map = beginningOffsets(partition.getTopic());
                for (Map.Entry<TopicPartition, Long> entry : map.entrySet()) {
                    if (entry.getKey().getVGroupId() == partition.getVGroupId()) {
                        seek(entry.getKey(), entry.getValue());
                    } else {
                        beginningOffsets.put(entry.getKey(), entry.getValue());
                    }
                }
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
                Map<TopicPartition, Long> map = endOffsets(partition.getTopic());
                for (Map.Entry<TopicPartition, Long> entry : map.entrySet()) {
                    if (entry.getKey().getVGroupId() == partition.getVGroupId()) {
                        seek(entry.getKey(), entry.getValue());
                    } else {
                        endOffsets.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    @Override
    public Set<TopicPartition> assignment() throws SQLException {
        Set<TopicPartition> set = new HashSet<>();
        for (String topic : subscription()) {
            Assignment[] topicAssignment = getAssignment(topic);
            set.addAll(Arrays.stream(topicAssignment).map(a -> new TopicPartition(topic, a.getVgId()))
                    .collect(Collectors.toSet()));
        }
        return set;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) throws SQLException {
        Request request = factory.generateCommitted(new TopicPartition[]{partition});
        CommittedResp resp = (CommittedResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer committed error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }
        return new OffsetAndMetadata(resp.getCommitted()[0], null);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) throws SQLException {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        TopicPartition[] topicPartitions = partitions.toArray(new TopicPartition[0]);
        Request request = factory.generateCommitted(topicPartitions);
        CommittedResp resp = (CommittedResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer committed error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }
        for (int i = 0; i < topicPartitions.length; i++) {
            map.put(topicPartitions[i], new OffsetAndMetadata(resp.getCommitted()[i], null));
        }
        return map;
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) throws SQLException {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue().offset() < 0) {
                continue;
            }
            Request request = factory.generateCommitOffset(entry.getKey(), entry.getValue().offset());
            CommitOffsetResp resp = (CommitOffsetResp) transport.send(request);
            if (Code.SUCCESS.getCode() != resp.getCode()) {
                throw new SQLException("consumer commit offset error, code: (0x" + Integer.toHexString(resp.getCode())
                        + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
            }
        }
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback<V> callback) {
        callback.onComplete(offsets, TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD));
    }

    private Assignment[] getAssignment(String topic) throws SQLException {
        Request request = factory.generateAssignment(topic);
        AssignmentResp resp = (AssignmentResp) transport.send(request);
        if (Code.SUCCESS.getCode() != resp.getCode()) {
            throw new SQLException("consumer assignment error, code: (0x" + Integer.toHexString(resp.getCode())
                    + "), message: " + resp.getMessage() + ", timing: " + resp.getTiming());
        }
        return resp.getAssignment();
    }
}
