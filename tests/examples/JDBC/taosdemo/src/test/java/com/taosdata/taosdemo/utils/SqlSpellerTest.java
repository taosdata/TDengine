package com.taosdata.taosdemo.utils;

import com.taosdata.taosdemo.domain.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SqlSpellerTest {

    @Test
    public void createDatabase() {
        HashMap<String, String> map = new HashMap<>();
        map.put("database", "jdbcdb");
        map.put("keep", "3650");
        map.put("days", "30");
        map.put("replica", "1");
        map.put("minRows", "100");
        map.put("maxRows", "1000");
        map.put("cache", "16");
        map.put("blocks", "8");
        map.put("precision", "ms");
        map.put("comp", "2");
        map.put("walLevel", "1");
        map.put("quorum", "1");
        map.put("fsync", "3000");
        map.put("update", "0");
        String sql = SqlSpeller.createDatabase(map);
        System.out.println(sql);
    }

    @Test
    public void createTableUsingSuperTable() {
        SubTableMeta subTableMeta = new SubTableMeta();
        subTableMeta.setDatabase("test");
        subTableMeta.setSupertable("weather");
        subTableMeta.setName("t1");
        List<TagValue> tags = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            tags.add(new TagValue("tag" + (i + 1), "nchar(64)"));
        }
        subTableMeta.setTags(tags);
        String sql = SqlSpeller.createTableUsingSuperTable(subTableMeta);
        System.out.println(sql);
    }

    @Test
    public void insertOneTableMultiValues() {
        String sql = SqlSpeller.insertOneTableMultiValues(tables.get(0));
        System.out.println(sql);
    }

    @Test
    public void insertOneTableMultiValuesUsingSuperTable() {
        String sql = SqlSpeller.insertOneTableMultiValuesUsingSuperTable(tables.get(0));
        System.out.println(sql);
    }

    @Test
    public void insertMultiTableMultiValues() {
        String sql = SqlSpeller.insertMultiSubTableMultiValues(tables);
        System.out.println(sql);
    }

    @Test
    public void insertMultiTableMultiValuesUsingSuperTable() {
        String sql = SqlSpeller.insertMultiTableMultiValuesUsingSuperTable(tables);
        System.out.println(sql);
    }

    private List<SubTableValue> tables;

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

    @Test
    public void createSuperTable() {
        SuperTableMeta superTableMeta = new SuperTableMeta();
        superTableMeta.setDatabase("test");
        superTableMeta.setName("weather");
        List<FieldMeta> fields = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            fields.add(new FieldMeta("f" + (i + 1), "int"));
        }
        superTableMeta.setFields(fields);
        List<TagMeta> tags = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            tags.add(new TagMeta("t" + (i + 1), "nchar(64)"));
        }
        superTableMeta.setTags(tags);

        String sql = SqlSpeller.createSuperTable(superTableMeta);
        System.out.println(sql);
    }

    @Test
    public void createTable() {
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
        String sql = SqlSpeller.createTable(table);
        System.out.println(sql);
    }


    @Test
    public void testInsertOneTableMultiValues() {
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

        String sql = SqlSpeller.insertOneTableMultiValues(table);
        System.out.println(sql);
    }

    @Test
    public void insertOneTableMultiValuesWithColumns() {
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

        String sql = SqlSpeller.insertOneTableMultiValuesWithColumns(tableValue);
        System.out.println(sql);
    }

    @Test
    public void insertMultiTableMultiValuesWithColumns() {
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

        String sql = SqlSpeller.insertMultiTableMultiValuesWithColumns(tables);
        System.out.println(sql);
    }

    @Test
    public void testInsertMultiTableMultiValues() {
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

        String sql = SqlSpeller.insertMultiTableMultiValues(tables);
        System.out.println(sql);
    }

}