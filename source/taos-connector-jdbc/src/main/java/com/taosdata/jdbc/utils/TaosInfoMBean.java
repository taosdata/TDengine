package com.taosdata.jdbc.utils;

public interface TaosInfoMBean {

    long getConnectOpen();

    long getConnectClose();

    long getConnect_active();

    long getStatementCount();

}
