package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.TagValue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubTableServiceTest {
    private SubTableService service;

    private List<SubTableMeta> subTables;

    @Before
    public void before() {
        subTables = new ArrayList<>();
        for (int i = 1; i <= 1; i++) {
            SubTableMeta subTableMeta = new SubTableMeta();
            subTableMeta.setDatabase("test");
            subTableMeta.setSupertable("weather");
            subTableMeta.setName("t" + i);
            List<TagValue> tags = new ArrayList<>();
            tags.add(new TagValue("location", "beijing"));
            tags.add(new TagValue("groupId", i));
            subTableMeta.setTags(tags);
            subTables.add(subTableMeta);
        }
    }

    @Test
    public void testCreateSubTable() {
        int count = service.createSubTable(subTables);
        System.out.println("count >>> " + count);
    }

    @Test
    public void testCreateSubTableList() {
        int count = service.createSubTable(subTables, 10);
        System.out.println("count >>> " + count);
    }
}