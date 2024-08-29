package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.utils.TaosConstants;

import java.util.ArrayList;
import java.util.List;

public class SuperTableMetaGenerator {

    // 创建超级表，使用指定SQL语句
    public static SuperTableMeta generate(String superTableSQL) {
        SuperTableMeta tableMeta = new SuperTableMeta();
        // for example : create table superTable (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)
        superTableSQL = superTableSQL.trim().toLowerCase();
        if (!superTableSQL.startsWith("create"))
            throw new RuntimeException("invalid create super table SQL");

        if (superTableSQL.contains("tags")) {
            String tagSQL = superTableSQL.substring(superTableSQL.indexOf("tags") + 4).trim();
            tagSQL = tagSQL.substring(tagSQL.indexOf("(") + 1, tagSQL.lastIndexOf(")"));
            String[] tagPairs = tagSQL.split(",");
            List<TagMeta> tagMetaList = new ArrayList<>();
            for (String tagPair : tagPairs) {
                String name = tagPair.trim().split("\\s+")[0];
                String type = tagPair.trim().split("\\s+")[1];
                tagMetaList.add(new TagMeta(name, type));
            }
            tableMeta.setTags(tagMetaList);
            superTableSQL = superTableSQL.substring(0, superTableSQL.indexOf("tags"));
        }
        if (superTableSQL.contains("(")) {
            String fieldSQL = superTableSQL.substring(superTableSQL.indexOf("(") + 1, superTableSQL.indexOf(")"));
            String[] fieldPairs = fieldSQL.split(",");
            List<FieldMeta> fieldList = new ArrayList<>();
            for (String fieldPair : fieldPairs) {
                String name = fieldPair.trim().split("\\s+")[0];
                String type = fieldPair.trim().split("\\s+")[1];
                fieldList.add(new FieldMeta(name, type));
            }
            tableMeta.setFields(fieldList);
            superTableSQL = superTableSQL.substring(0, superTableSQL.indexOf("("));
        }
        superTableSQL = superTableSQL.substring(superTableSQL.indexOf("table") + 5).trim();
        if (superTableSQL.contains(".")) {
            String database = superTableSQL.split("\\.")[0];
            tableMeta.setDatabase(database);
            superTableSQL = superTableSQL.substring(superTableSQL.indexOf(".") + 1);
        }
        tableMeta.setName(superTableSQL.trim());

        return tableMeta;
    }

    // 创建超级表,指定field和tag的个数
    public static SuperTableMeta generate(String database, String name, int fieldSize, String fieldPrefix, int tagSize, String tagPrefix) {
        if (fieldSize < 2 || tagSize < 1) {
            throw new RuntimeException("create super table but fieldSize less than 2 or tagSize less than 1");
        }
        SuperTableMeta tableMetadata = new SuperTableMeta();
        tableMetadata.setDatabase(database);
        tableMetadata.setName(name);
        // fields
        List<FieldMeta> fields = new ArrayList<>();
        fields.add(new FieldMeta("ts", "timestamp"));
        for (int i = 1; i <= fieldSize; i++) {
            fields.add(new FieldMeta(fieldPrefix + "" + i, TaosConstants.DATA_TYPES[i % TaosConstants.DATA_TYPES.length]));
        }
        tableMetadata.setFields(fields);
        // tags
        List<TagMeta> tags = new ArrayList<>();
        for (int i = 1; i <= tagSize; i++) {
            tags.add(new TagMeta(tagPrefix + "" + i, TaosConstants.DATA_TYPES[i % TaosConstants.DATA_TYPES.length]));
        }
        tableMetadata.setTags(tags);
        return tableMetadata;
    }
}
