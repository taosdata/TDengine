package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.TableMeta;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TableServiceTest {
    private TableService tableService;

    private List<TableMeta> tables;

    @Before
    public void before() {
        tables = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            TableMeta tableMeta = new TableMeta();
            tableMeta.setDatabase("test");
            tableMeta.setName("weather" + (i + 1));
            tables.add(tableMeta);
        }
    }

    @Test
    public void testCreate() {
        tableService.create(tables);
    }

}