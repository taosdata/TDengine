package com.taosdata.taosdemo.mapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DatabaseMapperTest {
    @Autowired
    private DatabaseMapper databaseMapper;

    @Test
    public void createDatabase() {
        databaseMapper.createDatabase("db_test");
    }

    @Test
    public void dropDatabase() {
        databaseMapper.dropDatabase("db_test");
    }

    @Test
    public void creatDatabaseWithParameters() {
        Map<String, String> map = new HashMap<>();
        map.put("dbname", "weather");
        map.put("keep", "3650");
        map.put("days", "30");
        map.put("replica", "1");
        databaseMapper.createDatabaseWithParameters(map);
    }

    @Test
    public void useDatabase() {
        databaseMapper.useDatabase("test");
    }
}