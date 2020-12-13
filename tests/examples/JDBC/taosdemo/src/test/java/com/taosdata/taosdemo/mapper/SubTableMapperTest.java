package com.taosdata.taosdemo.mapper;

import com.taosdata.taosdemo.domain.*;
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
public class SubTableMapperTest {
    @Autowired
    private SubTableMapper subTableMapper;
    private List<SubTableValue> tables;

    @Test
    public void createUsingSuperTable() {
        SubTableMeta subTableMeta = new SubTableMeta();
        subTableMeta.setDatabase("test");
        subTableMeta.setSupertable("weather");
        subTableMeta.setName("t1");
        List<TagValue> tags = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            tags.add(new TagValue("tag" + (i + 1), "nchar(64)"));
        }
        subTableMeta.setTags(tags);
        subTableMapper.createUsingSuperTable(subTableMeta);
    }

    @Test
    public void insertOneTableMultiValues() {
        subTableMapper.insertOneTableMultiValues(tables.get(0));
    }

    @Test
    public void insertOneTableMultiValuesUsingSuperTable() {
        subTableMapper.insertOneTableMultiValuesUsingSuperTable(tables.get(0));
    }


    @Test
    public void insertMultiTableMultiValues() {
        subTableMapper.insertMultiTableMultiValues(tables);
    }

    @Test
    public void insertMultiTableMultiValuesUsingSuperTable() {
        subTableMapper.insertMultiTableMultiValuesUsingSuperTable(tables);
    }


    @Before
    public void before() {
        tables = new ArrayList<>();
        for (int ind = 0; ind < 3; ind++) {

            SubTableValue table = new SubTableValue();
            table.setDatabase("test");
            // supertable
            table.setSupertable("weather");
            table.setName("t" + (ind + 1));
            // tags
            List<TagValue> tags = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                tags.add(new TagValue("tag" + (i + 1), "beijing"));
            }
            table.setTags(tags);
            // values
            List<RowValue> values = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                List<FieldValue> fields = new ArrayList<>();
                for (int j = 0; j < 4; j++) {
                    fields.add(new FieldValue("f" + (j + 1), (j + 1) * 10));
                }
                values.add(new RowValue(fields));
            }
            table.setValues(values);

            tables.add(table);
        }
    }

}