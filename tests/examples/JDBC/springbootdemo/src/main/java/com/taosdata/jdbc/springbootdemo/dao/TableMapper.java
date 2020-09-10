package com.taosdata.jdbc.springbootdemo.dao;

import com.taosdata.jdbc.springbootdemo.domain.TableMetadata;

public interface TableMapper {

    boolean createSTable(TableMetadata tableMetadata);
}