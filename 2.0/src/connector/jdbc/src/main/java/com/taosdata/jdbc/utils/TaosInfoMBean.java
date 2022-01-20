package com.taosdata.jdbc.utils;

public interface TaosInfoMBean {

    long getConnect_open();

    long getConnect_close();

    long getConnect_active();

    long getStatement_count();

}
