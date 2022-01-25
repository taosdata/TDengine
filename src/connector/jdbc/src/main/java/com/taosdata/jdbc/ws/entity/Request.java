package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

/**
 * send to taosadapter
 */
public class Request {
    private String action;
    private Payload args;

    public Request(String action, Payload args) {
        this.action = action;
        this.args = args;
    }

    public String id() {
        return action + "_" + args.getReqId();
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Payload getArgs() {
        return args;
    }

    public void setArgs(Payload args) {
        this.args = args;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public static Request generateConnect(String user, String password, String db) {
        long reqId = IdUtil.getId(Action.CONN.getAction());
        ConnectReq connectReq = new ConnectReq(reqId, user, password, db);
        return new Request(Action.CONN.getAction(), connectReq);
    }

    public static Request generateQuery(String sql) {
        long reqId = IdUtil.getId(Action.QUERY.getAction());
        QueryReq queryReq = new QueryReq(reqId, sql);
        return new Request(Action.QUERY.getAction(), queryReq);
    }

    public static Request generateFetch(long id) {
        long reqId = IdUtil.getId(Action.FETCH.getAction());
        FetchReq fetchReq = new FetchReq(reqId, id);
        return new Request(Action.FETCH.getAction(), fetchReq);
    }

    public static Request generateFetchJson(long id) {
        long reqId = IdUtil.getId(Action.FETCH_JSON.getAction());
        FetchReq fetchReq = new FetchReq(reqId, id);
        return new Request(Action.FETCH_JSON.getAction(), fetchReq);
    }

    public static Request generateFetchBlock(long id) {
        long reqId = IdUtil.getId(Action.FETCH_BLOCK.getAction());
        FetchReq fetchReq = new FetchReq(reqId, id);
        return new Request(Action.FETCH_BLOCK.getAction(), fetchReq);
    }
}

class Payload {
    @JSONField(name = "req_id")
    private final long reqId;

    public Payload(long reqId) {
        this.reqId = reqId;
    }

    public long getReqId() {
        return reqId;
    }
}

class ConnectReq extends Payload {
    private String user;
    private String password;
    private String db;

    public ConnectReq(long reqId, String user, String password, String db) {
        super(reqId);
        this.user = user;
        this.password = password;
        this.db = db;
    }

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

class QueryReq extends Payload {
    private String sql;

    public QueryReq(long reqId, String sql) {
        super(reqId);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}

class FetchReq extends Payload {
    private long id;

    public FetchReq(long reqId, long id) {
        super(reqId);
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}