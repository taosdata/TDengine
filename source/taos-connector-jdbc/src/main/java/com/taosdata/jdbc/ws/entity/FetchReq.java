package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.utils.UInt64Serializer;

public class FetchReq extends Payload {
    @JsonSerialize(using = UInt64Serializer.class)
    @JsonDeserialize(using = UInt64Deserializer.class)
    @JsonProperty("id")

    private long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
