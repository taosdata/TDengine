package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.SuperTableMapper;
import com.taosdata.taosdemo.dao.SuperTableMapperImpl;
import com.taosdata.taosdemo.domain.SuperTableMeta;

import javax.sql.DataSource;

public class SuperTableService {

    private SuperTableMapper superTableMapper;

    public SuperTableService(DataSource dataSource) {
        this.superTableMapper = new SuperTableMapperImpl(dataSource);
    }

    // Create super table, specifying the name and type of each field and each tag
    public void create(SuperTableMeta superTableMeta) {
        superTableMapper.createSuperTable(superTableMeta);
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}
