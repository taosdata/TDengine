package com.taosdata.jdbc.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class TaosInfo implements TaosInfoMBean {

    public AtomicInteger conn = new AtomicInteger();
    public AtomicInteger stmt = new AtomicInteger();

    @Override
    public int getConnectionCount() {
        return conn.get();
    }

    @Override
    public int getStatementCount() {
        return stmt.get();
    }
}
