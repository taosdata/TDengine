package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.utils.ProductUtil;
import com.taosdata.jdbc.utils.ReqId;

/**
 * connection request pojo
 */

public class ConnectReq extends Payload implements Printable {

    @JsonProperty("user")
    private String user;
    @JsonProperty("password")
    private String password;
    @JsonProperty("db")
    private String db;
    @JsonProperty("mode")
    private Integer mode;
    @JsonProperty("tz")
    private String tz;
    @JsonProperty("app")
    private String app;
    @JsonProperty("ip")
    private String ip;
    @JsonProperty("connector")
    private String connector;
    @JsonProperty("bearer_token")
    private String bearerToken;
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

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
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

    public String getBearerToken() {
        return bearerToken;
    }

    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }
    public ConnectReq(ConnectionParam param) {
        this.setReqId(ReqId.getReqID());
        this.setUser(param.getUser());
        this.setPassword(param.getPassword());
        this.setDb(param.getDatabase());
        this.setTz(param.getTz());
        this.setApp(param.getAppName());
        this.setIp(param.getAppIp());
        this.setConnector(ProductUtil.getWsConnectorVersion());
        this.setBearerToken(param.getBearerToken());

        // Currently, only BI mode is supported. The downstream interface value is 0, so a conversion is performed here.
        if(param.getConnectMode() == ConnectionParam.CONNECT_MODE_BI){
            this.setMode(0);
        }
    }
    @Override
    public String toPrintString() {
        return new StringBuilder("ConnectReq{")
                .append("user='").append(user).append('"')
                .append(", password='").append("******").append('"')
                .append(", bearerToken='").append("******").append('"')
                .append(", db='").append(db).append('"')
                .append(", mode=").append(mode)
                .append(", tz='").append(tz).append('"')
                .append(", app='").append(app).append('"')
                .append(", ip='").append(ip).append('"')
                .append(", connector='").append(connector).append('"')
                .append(", reqId=").append(getReqId())
                .append('}')
                .toString();
    }
}
