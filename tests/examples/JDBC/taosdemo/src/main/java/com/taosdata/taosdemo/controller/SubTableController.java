package com.taosdata.taosdemo.controller;

import com.taosdata.taosdemo.domain.TableValue;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SubTableController {

    @Autowired
    private TableService tableService;
    @Autowired
    private SuperTableService superTableService;

    //TODO: 使用supertable创建一个子表

    //TODO：使用supertable创建多个子表

    //TODO：使用supertable多线程创建子表

    //TODO：使用supertable多线程创建子表，指定子表的name_prefix，子表的数量，使用线程的个数

    /**
     * 创建表，超级表或者普通表
     **/


    /**
     * 创建超级表的子表
     **/
    @PostMapping("/{database}/{superTable}")
    public int createTable(@PathVariable("database") String database,
                           @PathVariable("superTable") String superTable,
                           @RequestBody TableValue tableMetadta) {
        tableMetadta.setDatabase(database);
        return 0;
    }


}
