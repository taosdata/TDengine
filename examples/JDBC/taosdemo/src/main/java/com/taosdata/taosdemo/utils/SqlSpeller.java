package com.taosdata.taosdemo.utils;

import com.taosdata.taosdemo.domain.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlSpeller {

    // create database if not exists xxx keep xx days xx replica xx cache xx...
    public static String createDatabase(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("create database if not exists ").append(map.get("database")).append(" ");
        if (map.containsKey("keep"))
            sb.append("keep ").append(map.get("keep")).append(" ");
        if (map.containsKey("days"))
            sb.append("days ").append(map.get("days")).append(" ");
        if (map.containsKey("replica"))
            sb.append("replica ").append(map.get("replica")).append(" ");
        if (map.containsKey("cache"))
            sb.append("cache ").append(map.get("cache")).append(" ");
        if (map.containsKey("blocks"))
            sb.append("blocks ").append(map.get("blocks")).append(" ");
        if (map.containsKey("minrows"))
            sb.append("minrows ").append(map.get("minrows")).append(" ");
        if (map.containsKey("maxrows"))
            sb.append("maxrows ").append(map.get("maxrows")).append(" ");
        if (map.containsKey("precision"))
            sb.append("precision ").append(map.get("precision")).append(" ");
        if (map.containsKey("comp"))
            sb.append("comp ").append(map.get("comp")).append(" ");
        if (map.containsKey("walLevel"))
            sb.append("walLevel ").append(map.get("walLevel")).append(" ");
        if (map.containsKey("quorum"))
            sb.append("quorum ").append(map.get("quorum")).append(" ");
        if (map.containsKey("fsync"))
            sb.append("fsync ").append(map.get("fsync")).append(" ");
        if (map.containsKey("update"))
            sb.append("update ").append(map.get("update")).append(" ");
        return sb.toString();
    }

    // create table if not exists xx.xx using xx.xx tags(x,x,x)
    public static String createTableUsingSuperTable(SubTableMeta subTableMeta) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ").append(subTableMeta.getDatabase()).append(".").append(subTableMeta.getName()).append(" ");
        sb.append("using ").append(subTableMeta.getDatabase()).append(".").append(subTableMeta.getSupertable()).append(" ");
//        String tagStr = subTableMeta.getTags().stream().filter(Objects::nonNull)
//                .map(tagValue -> tagValue.getName() + " '" + tagValue.getValue() + "' ")
//                .collect(Collectors.joining(",", "(", ")"));
        sb.append("tags ").append(tagValues(subTableMeta.getTags()));
        return sb.toString();
    }

    // insert into xx.xxx values(x,x,x),(x,x,x)...
    public static String insertOneTableMultiValues(SubTableValue subTableValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(subTableValue.getDatabase()).append(".").append(subTableValue.getName() + " ");
        sb.append("values").append(rowValues(subTableValue.getValues()));
        return sb.toString();
    }

    //f1, f2, f3
    private static String fieldValues(List<FieldValue> fields) {
        return IntStream.range(0, fields.size()).mapToObj(i -> {
            if (i == 0) {
                return "" + fields.get(i).getValue() + "";
            } else {
                return "'" + fields.get(i).getValue() + "'";
            }
        }).collect(Collectors.joining(",", "(", ")"));

//        return fields.stream()
//                .filter(Objects::nonNull)
//                .map(fieldValue -> "'" + fieldValue.getValue() + "'")
//                .collect(Collectors.joining(",", "(", ")"));
    }

    //(f1, f2, f3),(f1, f2, f3)
    private static String rowValues(List<RowValue> rowValues) {
        return rowValues.stream().filter(Objects::nonNull)
                .map(rowValue -> fieldValues(rowValue.getFields()))
                .collect(Collectors.joining(",", "", ""));
    }

    // insert into xx.xxx using xx.xx tags(x,x,x) values(x,x,x),(x,x,x)...
    public static String insertOneTableMultiValuesUsingSuperTable(SubTableValue subTableValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(subTableValue.getDatabase()).append(".").append(subTableValue.getName()).append(" ");
        sb.append("using ").append(subTableValue.getDatabase()).append(".").append(subTableValue.getSupertable()).append(" ");
        sb.append("tags ").append(tagValues(subTableValue.getTags()) + " ");
        sb.append("values ").append(rowValues(subTableValue.getValues()));
        return sb.toString();
    }

    // (t1,t2,t3...)
    private static String tagValues(List<TagValue> tags) {
        return tags.stream().filter(Objects::nonNull)
                .map(tagValue -> "'" + tagValue.getValue() + "'")
                .collect(Collectors.joining(",", "(", ")"));
    }

    // insert into xx.xx values(),(),()... xx.xx values(),()...
    public static String insertMultiSubTableMultiValues(List<SubTableValue> tables) {
        return "insert into " + tables.stream().filter(Objects::nonNull)
                .map(table -> table.getDatabase() + "." + table.getName() + " values " + rowValues(table.getValues()))
                .collect(Collectors.joining(" ", "", ""));
    }

    // insert into xx.xx using xx.xx tags(xx,xx) values(),()...
    public static String insertMultiTableMultiValuesUsingSuperTable(List<SubTableValue> tables) {
        return "insert into " + tables.stream().filter(Objects::nonNull)
                .map(table -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(table.getDatabase()).append(".").append(table.getName());
                    sb.append(" using ").append(table.getDatabase()).append(".").append(table.getSupertable());
                    sb.append(" tags ").append(tagValues(table.getTags()));
                    sb.append(" values ").append(rowValues(table.getValues()));
                    return sb.toString();
                }).collect(Collectors.joining(" "));
    }

    // create table if not exists xx.xx (f1 xx,f2 xx...) tags(t1 xx, t2 xx...)
    public static String createSuperTable(SuperTableMeta tableMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ").append(tableMetadata.getDatabase()).append(".").append(tableMetadata.getName());
        String fields = tableMetadata.getFields().stream()
                .filter(Objects::nonNull).map(field -> field.getName() + " " + field.getType() + " ")
                .collect(Collectors.joining(",", "(", ")"));
        sb.append(fields);
        sb.append(" tags ");
        String tags = tableMetadata.getTags().stream().filter(Objects::nonNull)
                .map(tag -> tag.getName() + " " + tag.getType() + " ")
                .collect(Collectors.joining(",", "(", ")"));
        sb.append(tags);
        return sb.toString();
    }


    public static String createTable(TableMeta tableMeta) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists ").append(tableMeta.getDatabase()).append(".").append(tableMeta.getName()).append(" ");
        String fields = tableMeta.getFields().stream()
                .filter(Objects::nonNull).map(field -> field.getName() + " " + field.getType() + " ")
                .collect(Collectors.joining(",", "(", ")"));
        sb.append(fields);
        return sb.toString();
    }

    // insert into xx.xx values()
    public static String insertOneTableMultiValues(TableValue table) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table.getDatabase()).append(".").append(table.getName() + " ");
        sb.append("values").append(rowValues(table.getValues()));
        return sb.toString();

    }

    // insert into xx.xx (f1, f2, f3...) values(xx,xx,xx),(xx,xx,xx)...
    public static String insertOneTableMultiValuesWithColumns(TableValue table) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table.getDatabase()).append(".").append(table.getName()).append(" ");
        sb.append(columnNames(table.getColumns()));
        sb.append(" values ").append(rowValues(table.getValues()));
        return sb.toString();
    }

    // (f1, f2, f3...)
    private static String columnNames(List<FieldMeta> fields) {
        return fields.stream()
                .filter(Objects::nonNull)
                .map(column -> column.getName() + " ")
                .collect(Collectors.joining(",", "(", ")"));
    }

    public static String insertMultiTableMultiValuesWithColumns(List<TableValue> tables) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tables.stream().filter(Objects::nonNull)
                .map(table -> table.getDatabase() + "." + table.getName() + " " + columnNames(table.getColumns()) + " values " + rowValues(table.getValues()))
                .collect(Collectors.joining(" ")));
        return sb.toString();
    }

    public static String insertMultiTableMultiValues(List<TableValue> tables) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tables.stream().filter(Objects::nonNull).map(table ->
                table.getDatabase() + "." + table.getName() + " values " + rowValues(table.getValues())
        ).collect(Collectors.joining(" ")));
        return sb.toString();
    }
}
