package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.entity.VersionResp;
import com.taosdata.jdbc.ws.tmq.entity.*;

import java.util.HashMap;
import java.util.Map;

public enum ConsumerAction {
    // subscribe
    VERSION("version", VersionResp.class),
    SUBSCRIBE("subscribe", SubscribeResp.class),
    POLL("poll", PollResp.class),
    FETCH_RAW_DATA("fetch_raw_data", FetchRawBlockResp.class),
    COMMIT("commit", CommitResp.class),
    ASSIGNMENT("assignment", AssignmentResp.class),
    SEEK("seek", SeekResp.class),
    UNSUBSCRIBE("unsubscribe", UnsubscribeResp.class),
    COMMIT_OFFSET("commit_offset", CommitOffsetResp.class),
    COMMITTED("committed", CommittedResp.class),
    POSITION("position", PositionResp.class),
    LIST_TOPICS("list_topics", ListTopicsResp.class),
    FETCH_JSON_META("fetch_json_meta", FetchJsonMetaResp.class),

    ;

    private final String action;
    private final Class<? extends Response> clazz;

    ConsumerAction(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getResponseClazz() {
        return clazz;
    }

    private static final Map<String, ConsumerAction> actions = new HashMap<>();

    static {
        for (ConsumerAction value : ConsumerAction.values()) {
            actions.put(value.action, value);
        }
    }

    public static ConsumerAction of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}
