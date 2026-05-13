package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * connection result pojo
 */
public class VersionResp extends CommonResp {
    @JsonProperty("version")
    String version;
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
