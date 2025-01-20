package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.domain.TagValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TagValueGeneratorTest {
    List<TagValue> tagvalues;

    @Test
    public void generate() {
        List<TagMeta> tagMetaList = new ArrayList<>();
        tagMetaList.add(new TagMeta("location", "nchar(10)"));
        tagMetaList.add(new TagMeta("groupId", "int"));
        tagMetaList.add(new TagMeta("ts", "timestamp"));
        tagMetaList.add(new TagMeta("temperature", "float"));
        tagMetaList.add(new TagMeta("humidity", "double"));
        tagMetaList.add(new TagMeta("text", "binary(10)"));
        tagvalues = TagValueGenerator.generate(tagMetaList);
        Assert.assertEquals("location", tagvalues.get(0).getName());
        Assert.assertEquals("groupId", tagvalues.get(1).getName());
        Assert.assertEquals("ts", tagvalues.get(2).getName());
        Assert.assertEquals("temperature", tagvalues.get(3).getName());
        Assert.assertEquals("humidity", tagvalues.get(4).getName());
        Assert.assertEquals("text", tagvalues.get(5).getName());
    }

    @After
    public void after() {
        tagvalues.stream().forEach(System.out::println);
    }
}