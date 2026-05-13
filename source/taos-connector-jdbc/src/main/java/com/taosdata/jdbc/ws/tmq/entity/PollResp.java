package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class PollResp extends CommonResp {
    @JsonProperty("have_message")
    private boolean haveMessage;
    @JsonProperty("topic")
    private String topic;
    @JsonProperty("database")
    private String database;
    @JsonProperty("vgroup_id")
    private int vgroupId;
    @JsonProperty("message_type")
    private int messageType;
    @JsonProperty("message_id")
    @JsonDeserialize(using = UInt64Deserializer.class)
    private long messageId;
    @JsonProperty("offset")
    private long offset;
    @JsonProperty("timing")
    private long timing;

    public boolean isHaveMessage() {
        return haveMessage;
    }

    public void setHaveMessage(boolean haveMessage) {
        this.haveMessage = haveMessage;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public int getVgroupId() {
        return vgroupId;
    }

    public void setVgroupId(int vgroupId) {
        this.vgroupId = vgroupId;
    }

    public int getMessageType() {
        return messageType;
    }

    public void setMessageType(int messageType) {
        this.messageType = messageType;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
