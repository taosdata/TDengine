package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class FetchJsonMetaResp extends CommonResp {
    @JsonProperty("timing")
    private long timing;
    @JsonProperty("message_id")
    @JsonDeserialize(using = UInt64Deserializer.class)
    private long messageId;

    @JsonProperty("data")
    private FetchJsonMetaData data;

    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public FetchJsonMetaData getData() {
        return data;
    }

    public void setData(FetchJsonMetaData data) {
        this.data = data;
    }
}
