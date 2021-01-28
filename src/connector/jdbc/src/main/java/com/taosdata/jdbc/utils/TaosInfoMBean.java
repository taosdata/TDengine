package com.taosdata.jdbc.utils;

public interface TaosInfoMBean {

    int getConnectionCount();

    int getStatementCount();
}
