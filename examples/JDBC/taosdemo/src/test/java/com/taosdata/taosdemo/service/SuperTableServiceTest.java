package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SuperTableServiceTest {

    private SuperTableService service;

    @Test
    public void testCreate() {
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
        service.create(superTableMeta);
    }

}