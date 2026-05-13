package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.Payload;

public class PollReq extends Payload {
    @JsonProperty("blocking_time")
    private long blockingTime;

    @JsonProperty("message_id")
    @JsonDeserialize(using = UInt64Deserializer.class)
    private long messageId;

    public long getBlockingTime() {
        return blockingTime;
    }

    public void setBlockingTime(long blockingTime) {
        this.blockingTime = blockingTime;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }
}
