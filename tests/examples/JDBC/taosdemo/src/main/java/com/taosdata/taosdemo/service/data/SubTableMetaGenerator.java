package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagValue;

import java.util.ArrayList;
import java.util.List;

public class SubTableMetaGenerator {

    // 创建tableSize张子表，使用tablePrefix作为子表名的前缀，使用superTableMeta的元数据
    // create table xxx using XXX tags(XXX)
    public static List<SubTableMeta> generate(SuperTableMeta superTableMeta, int tableSize, String tablePrefix) {
        List<SubTableMeta> subTableMetaList = new ArrayList<>();
        for (int i = 1; i <= tableSize; i++) {
            SubTableMeta subTableMeta = new SubTableMeta();
            // create table xxx.xxx using xxx tags(...)
            subTableMeta.setDatabase(superTableMeta.getDatabase());
            subTableMeta.setName(tablePrefix + i);
            subTableMeta.setSupertable(superTableMeta.getName());
            subTableMeta.setFields(superTableMeta.getFields());
            List<TagValue> tagValues = TagValueGenerator.generate(superTableMeta.getTags());
            subTableMeta.setTags(tagValues);
            subTableMetaList.add(subTableMeta);
        }
        return subTableMetaList;
    }

}
