package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.DatabasePrecision;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SchemaConfigurationParserTest {

    private String json;

    @Test
    public void parseSchema() {
        // given
        JSONObject schemaJSON = JSONObject.parseObject(json);
        // when
        SchemaConfiguration configuration = (SchemaConfiguration) ConfigurationFactory.build(ConfigurationType.SCHEMA, schemaJSON);
        // then
        // assert database
        List<Configuration> databases = configuration.find(ConfigurationType.DATABASE);
        Assert.assertEquals(1, databases.size());
        DatabaseConfiguration database = (DatabaseConfiguration) databases.get(0);
        Assert.assertEquals("test", database.getName());
        Assert.assertEquals(DatabasePrecision.ms, database.getPrecision());
        // assert stable
        List<Configuration> stables = configuration.find(ConfigurationType.STABLE);
        Assert.assertEquals(1, stables.size());
        StableConfiguration stable = (StableConfiguration) stables.get(0);
        Assert.assertEquals("weather", stable.getName());
        Assert.assertEquals(new Long(100), stable.getTables());
        // assert columns
        List<Configuration> columns = stable.find(ConfigurationType.COLUMN);
        Assert.assertEquals(3, columns.size());
        ColumnConfiguration column = (ColumnConfiguration) columns.get(1);
        Assert.assertEquals("temperature", column.getName());
        Assert.assertEquals("float", column.getType());
        Assert.assertNull(column.getLength());
        // assert tags
        List<Configuration> tags = stable.find(ConfigurationType.TAG);
        Assert.assertEquals(2, tags.size());
        TagConfiguration tag = (TagConfiguration) tags.get(1);
        Assert.assertEquals("groupId", tag.getName());
        Assert.assertEquals("int", tag.getType());
    }

    @Before
    public void before() throws IOException {
        json = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("schema.json")));
    }

}