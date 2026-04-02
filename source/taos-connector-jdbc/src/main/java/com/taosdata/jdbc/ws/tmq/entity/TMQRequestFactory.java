package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.tmq.TopicPartition;
import com.taosdata.jdbc.utils.ProductUtil;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.tmq.ConsumerAction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * generate id for request
 */
public class TMQRequestFactory {
    private final Map<String, AtomicLong> ids = new HashMap<>();

    public long getId(String action) {
        return ids.get(action).incrementAndGet();
    }

    public TMQRequestFactory() {
        for (ConsumerAction value : ConsumerAction.values()) {
            ids.put(value.getAction(), new AtomicLong(0));
        }
    }

    public Request generateSubscribe(ConsumerParam param, String[] topics
            , String enableAutoCommit) {
        long reqId = this.getId(ConsumerAction.SUBSCRIBE.getAction());

        SubscribeReq subscribeReq = new SubscribeReq();

        subscribeReq.setReqId(reqId);
        subscribeReq.setUser(param.getConnectionParam().getUser());
        subscribeReq.setPassword(param.getConnectionParam().getPassword());

        // Set bearerToken if td.connect.token is provided
        String token = param.getConfig().get(com.taosdata.jdbc.tmq.TMQConstants.TMQ_CONNECT_TOKEN);
        if (token != null && !token.isEmpty()) {
            subscribeReq.setBearerToken(token);
        }

        subscribeReq.setDb(param.getConnectionParam().getDatabase());
        subscribeReq.setGroupId(param.getGroupId());
        subscribeReq.setClientId(param.getClientId());
        subscribeReq.setOffsetRest(param.getOffsetRest());
        subscribeReq.setTopics(topics);
        subscribeReq.setAutoCommit(enableAutoCommit);
        subscribeReq.setWithTableName(param.getMsgWithTableName());
        subscribeReq.setTz(param.getConnectionParam().getTz());
        subscribeReq.setApp(param.getConnectionParam().getAppName());
        subscribeReq.setIp(param.getConnectionParam().getAppIp());
        subscribeReq.setConnector(ProductUtil.getWsConnectorVersion());
        subscribeReq.setEnableBatchMeta(param.getEnableBatchMeta());
        subscribeReq.setConfig(param.getConfig());

        return new Request(ConsumerAction.SUBSCRIBE.getAction(), subscribeReq);
    }

    public Request generatePoll(long messageId, long blockingTime) {
        long reqId = this.getId(ConsumerAction.POLL.getAction());
        PollReq pollReq = new PollReq();
        pollReq.setReqId(reqId);
        pollReq.setMessageId(messageId);
        pollReq.setBlockingTime(blockingTime);
        return new Request(ConsumerAction.POLL.getAction(), pollReq);
    }

    public Request generateFetchRaw(long messageId) {
        long reqId = this.getId(ConsumerAction.FETCH_RAW_DATA.getAction());
        FetchRawReq fetchReq = new FetchRawReq();
        fetchReq.setReqId(reqId);
        fetchReq.setMessageId(messageId);
        return new Request(ConsumerAction.FETCH_RAW_DATA.getAction(), fetchReq);
    }

    public Request generateCommit(long messageId) {
        long reqId = this.getId(ConsumerAction.COMMIT.getAction());
        CommitReq commitReq = new CommitReq();
        commitReq.setReqId(reqId);
        commitReq.setMessageId(messageId);
        return new Request(ConsumerAction.COMMIT.getAction(), commitReq);
    }

    public Request generateSeek(String topic, int vgId, long offset) {
        long reqId = this.getId(ConsumerAction.SEEK.getAction());
        SeekReq seekReq = new SeekReq();
        seekReq.setReqId(reqId);
        seekReq.setTopic(topic);
        seekReq.setVgId(vgId);
        seekReq.setOffset(offset);
        return new Request(ConsumerAction.SEEK.getAction(), seekReq);
    }

    public Request generateAssignment(String topic) {
        long reqId = this.getId(ConsumerAction.ASSIGNMENT.getAction());
        AssignmentReq assignmentReq = new AssignmentReq();
        assignmentReq.setReqId(reqId);
        assignmentReq.setTopic(topic);
        return new Request(ConsumerAction.ASSIGNMENT.getAction(), assignmentReq);
    }

    public Request generateUnsubscribe() {
        long reqId = this.getId(ConsumerAction.UNSUBSCRIBE.getAction());
        UnsubscribeReq unsubscribeReq = new UnsubscribeReq();
        unsubscribeReq.setReqId(reqId);
        return new Request(ConsumerAction.UNSUBSCRIBE.getAction(), unsubscribeReq);
    }

    public Request generateCommitted(TopicPartition[] topicPartitions) {
        long reqId = this.getId(ConsumerAction.COMMITTED.getAction());
        CommittedReq committedReq = new CommittedReq();
        committedReq.setReqId(reqId);
        committedReq.setTopicPartitions(topicPartitions);
        return new Request(ConsumerAction.COMMITTED.getAction(), committedReq);
    }

    public Request generatePosition(TopicPartition[] topicPartitions){
        long reqId = this.getId(ConsumerAction.POSITION.getAction());
        PositionReq positionReq = new PositionReq();
        positionReq.setReqId(reqId);
        positionReq.setTopicPartitions(topicPartitions);
        return new Request(ConsumerAction.POSITION.getAction(), positionReq);
    }

    public Request generateSubscription(){
        long reqId = this.getId(ConsumerAction.LIST_TOPICS.getAction());
        ListTopicsReq listTopicsReq = new ListTopicsReq();
        listTopicsReq.setReqId(reqId);
        return new Request(ConsumerAction.LIST_TOPICS.getAction(), listTopicsReq);
    }

    public Request generateCommitOffset(TopicPartition topicPartition, long offset){
        long reqId = this.getId(ConsumerAction.COMMIT_OFFSET.getAction());
        CommitOffsetReq commitOffsetReq = new CommitOffsetReq();
        commitOffsetReq.setReqId(reqId);
        commitOffsetReq.setTopic(topicPartition.getTopic());
        commitOffsetReq.setVgroupId(topicPartition.getVGroupId());
        commitOffsetReq.setOffset(offset);
        return new Request(ConsumerAction.COMMIT_OFFSET.getAction(), commitOffsetReq);
    }

    public Request generateFetchJsonMeata(long messageId){
        long reqId = this.getId(ConsumerAction.FETCH_JSON_META.getAction());
        FetchJsonMetaReq fetchJsonMetaReq = new FetchJsonMetaReq();
        fetchJsonMetaReq.setReqId(reqId);
        fetchJsonMetaReq.setMessageId(messageId);
        return new Request(ConsumerAction.FETCH_JSON_META.getAction(), fetchJsonMetaReq);
    }
}
