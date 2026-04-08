package com.taosdata.tsync.service;

public class AffectRowsProcessService implements ResultProcessService {
    private long affectRows = 0;

    @Override
    public void process(Object result) {
        affectRows += new Long(result.toString());
    }

    @Override
    public Object getResult() {
        return affectRows;
    }
}
