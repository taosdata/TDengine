package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * connection result pojo
 */
public class ConnectResp extends CommonResp {
    @JsonProperty("version")
    String version;
    @JsonProperty("list_instances")
    private String[] listInstances;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String[] getListInstances() {
        return listInstances;
    }

    public void setListInstances(String[] listInstances) {
        this.listInstances = listInstances;
    }
}
