package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.ws.entity.Payload;

import java.util.HashMap;

public class SubscribeReq extends Payload implements Printable {
    @JsonProperty("user")
    private String user;
    @JsonProperty("password")
    private String password;
    @JsonProperty("db")
    private String db;
    @JsonProperty("group_id")
    private String groupId;
    @JsonProperty("client_id")
    private String clientId;
    @JsonProperty("offset_rest")
    private String offsetRest;
    @JsonProperty("topics")
    private String[] topics;
    @JsonProperty("auto_commit")
    private String autoCommit;
    @JsonProperty("auto_commit_interval_ms")
    private String autoCommitIntervalMs;
    @JsonProperty("with_table_name")
    private String withTableName;
    @JsonProperty("enable_batch_meta")
    private String enableBatchMeta;
    @JsonProperty("tz")
    private String tz;
    @JsonProperty("app")
    private String app;
    @JsonProperty("ip")
    private String ip;
    @JsonProperty("connector")
    private String connector;
    @JsonProperty("td.connect.token")
    private String bearerToken;
    @JsonProperty("config")
    private HashMap<String, String> config = new HashMap<>();

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getOffsetRest() {
        return offsetRest;
    }

    public void setOffsetRest(String offsetRest) {
        this.offsetRest = offsetRest;
    }

    public String[] getTopics() {
        return topics;
    }

    public void setTopics(String[] topics) {
        this.topics = topics;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(String autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(String autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public String getWithTableName() {
        return withTableName;
    }

    public void setWithTableName(String withTableName) {
        this.withTableName = withTableName;
    }

    public String getTz() {
        return tz;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getEnableBatchMeta() {
        return enableBatchMeta;
    }

    public void setEnableBatchMeta(String enableBatchMeta) {
        this.enableBatchMeta = enableBatchMeta;
    }


    public HashMap<String, String> getConfig() {
        return config;
    }

    public void setConfig(HashMap<String, String> config) {
        this.config = config;
    }
    public String getBearerToken() {
        return bearerToken;
    }

    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    public String toPrintString() {

        String topicsStr = this.topics == null ? "" : String.join(",", this.topics);
        return new StringBuilder("SubscribeReq{")
                .append("user='").append(user).append('"')
                .append(", password='").append("******").append('"')
                .append(", bearerToken='").append("******").append('"')
                .append(", db='").append(db).append('"')
                .append(", groupId='").append(groupId).append('"')
                .append(", clientId='").append(clientId).append('"')
                .append(", offsetRest='").append(offsetRest).append('"')
                .append(", topics=").append(topicsStr)
                .append(", autoCommit='").append(autoCommit).append('"')
                .append(", autoCommitIntervalMs='").append(autoCommitIntervalMs).append('"')
                .append(", withTableName='").append(withTableName).append('"')
                .append(", enableBatchMeta='").append(enableBatchMeta).append('"')
                .append(", tz='").append(tz).append('"')
                .append(", app='").append(app).append('"')
                .append(", ip='").append(ip).append('"')
                .append(", connector='").append(connector).append('"')
                .append(", reqId=").append(getReqId())
                .append('}')
                .toString();
    }
}
