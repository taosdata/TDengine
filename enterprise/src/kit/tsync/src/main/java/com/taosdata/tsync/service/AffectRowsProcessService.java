package com.taosdata.tsync.service;

public class AffectRowsProcessService implements ResultProcessService {
    private Integer affectRows = 0;

    @Override
    public void process(Object result) {
        if (result instanceof Integer)
            affectRows += (Integer) result;
    }

    @Override
    public Object getResult() {
        return affectRows;
    }
}
