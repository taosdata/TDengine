package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.TableMapper;
import com.taosdata.taosdemo.domain.TableMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TableService extends AbstractService {

    private TableMapper tableMapper;

    //创建一张表
    public void create(TableMeta tableMeta) {
        tableMapper.create(tableMeta);
    }

    //创建多张表
    public void create(List<TableMeta> tables) {
        tables.stream().forEach(this::create);
    }


}
