package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.TableMeta;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TableServiceTest {
    @Autowired
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
        int count = tableService.create(tables);
        System.out.println(count);
    }

    @Test
    public void testCreateMultiThreads() {
        System.out.println(tableService.create(tables, 10));
    }
}