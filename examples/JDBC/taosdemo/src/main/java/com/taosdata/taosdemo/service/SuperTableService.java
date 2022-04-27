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

    // 创建超级表，指定每个field的名称和类型，每个tag的名称和类型
    public void create(SuperTableMeta superTableMeta) {
        superTableMapper.createSuperTable(superTableMeta);
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}
