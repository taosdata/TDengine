package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SuperTableServiceTest {

    @Autowired
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