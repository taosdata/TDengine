package com.taosdata.taosdemo.mapper;

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
public class SuperTableMapperTest {
    @Autowired
    private SuperTableMapper superTableMapper;

    @Test
    public void testCreateSuperTableUsingSQL() {
        String sql = "create table test.weather (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
        superTableMapper.createSuperTableUsingSQL(sql);
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

        superTableMapper.createSuperTable(superTableMeta);
    }

    @Test
    public void dropSuperTable() {
        superTableMapper.dropSuperTable("test", "weather");
    }
}