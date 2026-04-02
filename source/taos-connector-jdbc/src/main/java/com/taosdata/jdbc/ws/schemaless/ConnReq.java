package com.taosdata.jdbc.ws.schemaless;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class ConnReq extends Payload {
    @JsonProperty("user")
    private String user;
    @JsonProperty("password")
    private String password;
    @JsonProperty("db")
    private String db;

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
}
