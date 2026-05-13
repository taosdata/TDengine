package com.taosdata.jdbc.enums;

public enum FetchState {
    STOPPED(1),
    FETCHING(2),
    FINISHED_ERROR(3),
    ;
    private final long state;

    FetchState(int state) {
        this.state = state;
    }

    public long get() {
        return state;
    }
}
