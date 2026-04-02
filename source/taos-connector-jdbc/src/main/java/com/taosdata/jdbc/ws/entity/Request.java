package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * send to taosadapter
 */
public class Request implements Printable {
    private static final Logger log = LoggerFactory.getLogger(Request.class);

    @JsonProperty("action")
    private String action;
    @JsonProperty("args")
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
        ObjectMapper objectMapper = JsonUtil.getObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Request to string error", e);
            return "";
        }
    }

    @Override
    public String toPrintString() {
         if (args instanceof Printable) {
           return ((Printable) args).toPrintString();
        } else {
            return this.toString();
        }
    }
}