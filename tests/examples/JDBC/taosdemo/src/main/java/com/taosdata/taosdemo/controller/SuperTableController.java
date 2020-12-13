package com.taosdata.taosdemo.controller;

import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.service.SuperTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

public class SuperTableController {
    @Autowired
    private SuperTableService superTableService;


    @PostMapping("/{database}")
    public int createTable(@PathVariable("database") String database, @RequestBody SuperTableMeta tableMetadta) {
        tableMetadta.setDatabase(database);
        return superTableService.create(tableMetadta);
    }

    //TODO: 删除超级表

    //TODO：查询超级表

    //TODO：统计查询表
}
