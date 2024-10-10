package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.TableMapper;
import com.taosdata.taosdemo.domain.TableMeta;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TableService extends AbstractService {

    private TableMapper tableMapper;

    // Create a table
    public void create(TableMeta tableMeta) {
        tableMapper.create(tableMeta);
    }

    // Create multiple tables
    public void create(List<TableMeta> tables) {
        tables.stream().forEach(this::create);
    }

}
