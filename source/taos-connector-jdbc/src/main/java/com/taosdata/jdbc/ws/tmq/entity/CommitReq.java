package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.taosdata.jdbc.utils.UInt64Serializer;
import com.taosdata.jdbc.ws.entity.Payload;

public class CommitReq extends Payload {

    @JsonProperty("message_id")
    @JsonSerialize(using = UInt64Serializer.class)
    private long messageId;

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }
}
