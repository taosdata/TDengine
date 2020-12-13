package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.mapper.SuperTableMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SuperTableService {

    @Autowired
    private SuperTableMapper superTableMapper;

    // 创建超级表，指定每个field的名称和类型，每个tag的名称和类型
    public int create(SuperTableMeta superTableMeta) {
        return superTableMapper.createSuperTable(superTableMeta);
    }

    public void drop(String database, String name) {
        superTableMapper.dropSuperTable(database, name);
    }
}
