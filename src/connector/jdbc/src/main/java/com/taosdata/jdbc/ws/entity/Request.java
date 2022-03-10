package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.JSON;

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

    public String getAction() {
        return action;
    }

    public Long id(){
        return args.getReqId();
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
}