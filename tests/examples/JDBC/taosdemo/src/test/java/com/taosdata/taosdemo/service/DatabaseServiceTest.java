package com.taosdata.taosdemo.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DatabaseServiceTest {
    @Autowired
    private DatabaseService service;

    @Test
    public void testCreateDatabase1() {
        service.createDatabase("testXXXX");
    }

    @Test
    public void dropDatabase() {
        service.dropDatabase("testXXXX");
    }

    @Test
    public void useDatabase() {
        service.useDatabase("test");
    }
}