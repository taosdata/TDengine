package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubTableMetaGeneratorTest {
    List<SubTableMeta> subTableMetas;

    @Test
    public void generate() {
        SuperTableMeta superTableMeta = new SuperTableMeta();
        superTableMeta.setDatabase("test");
        superTableMeta.setName("weather");
        List<FieldMeta> fields = new ArrayList<>();
        fields.add(new FieldMeta("ts", "timestamp"));
        fields.add(new FieldMeta("temperature", "float"));
        fields.add(new FieldMeta("humidity", "int"));
        superTableMeta.setFields(fields);
        List<TagMeta> tags = new ArrayList<>();
        tags.add(new TagMeta("location", "nchar(64)"));
        tags.add(new TagMeta("groupId", "int"));
        superTableMeta.setTags(tags);

        subTableMetas = SubTableMetaGenerator.generate(superTableMeta, 10, "t");
        Assert.assertEquals(10, subTableMetas.size());
        Assert.assertEquals("t1", subTableMetas.get(0).getName());
        Assert.assertEquals("t2", subTableMetas.get(1).getName());
        Assert.assertEquals("t3", subTableMetas.get(2).getName());
        Assert.assertEquals("t4", subTableMetas.get(3).getName());
        Assert.assertEquals("t5", subTableMetas.get(4).getName());
        Assert.assertEquals("t6", subTableMetas.get(5).getName());
        Assert.assertEquals("t7", subTableMetas.get(6).getName());
        Assert.assertEquals("t8", subTableMetas.get(7).getName());
        Assert.assertEquals("t9", subTableMetas.get(8).getName());
        Assert.assertEquals("t10", subTableMetas.get(9).getName());
    }

    @After
    public void after() {
        for (SubTableMeta subTableMeta : subTableMetas) {
            System.out.println(subTableMeta);
        }
    }
}