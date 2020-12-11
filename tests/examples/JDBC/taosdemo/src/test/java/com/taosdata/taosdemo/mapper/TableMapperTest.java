package com.taosdata.taosdemo.mapper;

import com.taosdata.taosdemo.domain.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TableMapperTest {
    @Autowired
    private TableMapper tableMapper;
    private static Random random = new Random(System.currentTimeMillis());

    @Test
    public void create() {
        TableMeta table = new TableMeta();
        table.setDatabase("test");
        table.setName("t1");
        List<FieldMeta> fields = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            FieldMeta field = new FieldMeta();
            field.setName("f" + (i + 1));
            field.setType("nchar(64)");
            fields.add(field);
        }
        table.setFields(fields);
        tableMapper.create(table);
    }

    @Test
    public void insertOneTableMultiValues() {
        TableValue table = new TableValue();
        table.setDatabase("test");
        table.setName("t1");
        List<RowValue> values = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            List<FieldValue> fields = new ArrayList<>();
            for (int k = 0; k < 2; k++) {
                FieldValue field = new FieldValue<>();
                field.setValue((k + 1) * 100);
                fields.add(field);
            }
            values.add(new RowValue(fields));
        }
        table.setValues(values);

        tableMapper.insertOneTableMultiValues(table);
    }

    @Test
    public void insertOneTableMultiValuesWithCoulmns() {
        TableValue tableValue = new TableValue();
        tableValue.setDatabase("test");
        tableValue.setName("weather");
        // columns
        List<FieldMeta> columns = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            FieldMeta field = new FieldMeta();
            field.setName("f" + (i + 1));
            columns.add(field);
        }
        tableValue.setColumns(columns);
        // values
        List<RowValue> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<FieldValue> fields = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                FieldValue field = new FieldValue();
                field.setValue(j);
                fields.add(field);
            }
            values.add(new RowValue(fields));
        }
        tableValue.setValues(values);
        tableMapper.insertOneTableMultiValuesWithColumns(tableValue);
    }

    @Test
    public void insertMultiTableMultiValues() {
        List<TableValue> tables = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TableValue table = new TableValue();
            table.setDatabase("test");
            table.setName("t" + (i + 1));
            List<RowValue> values = new ArrayList<>();
            for (int j = 0; j < 5; j++) {
                List<FieldValue> fields = new ArrayList<>();
                for (int k = 0; k < 2; k++) {
                    FieldValue field = new FieldValue<>();
                    field.setValue((k + 1) * 10);
                    fields.add(field);
                }
                values.add(new RowValue(fields));
            }
            table.setValues(values);

            tables.add(table);
        }
        tableMapper.insertMultiTableMultiValues(tables);
    }

    @Test
    public void insertMultiTableMultiValuesWithCoulumns() {
        List<TableValue> tables = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TableValue table = new TableValue();
            table.setDatabase("test");
            table.setName("t" + (i + 1));
            // columns
            List<FieldMeta> columns = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                FieldMeta field = new FieldMeta();
                field.setName("f" + (j + 1));
                columns.add(field);
            }
            table.setColumns(columns);
            // values
            List<RowValue> values = new ArrayList<>();
            for (int j = 0; j < 5; j++) {
                List<FieldValue> fields = new ArrayList<>();
                for (int k = 0; k < columns.size(); k++) {
                    FieldValue field = new FieldValue<>();
                    field.setValue((k + 1) * 10);
                    fields.add(field);
                }
                values.add(new RowValue(fields));
            }
            table.setValues(values);

            tables.add(table);
        }
        tableMapper.insertMultiTableMultiValuesWithColumns(tables);
    }

}