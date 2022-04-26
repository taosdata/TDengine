package com.taosdata.taosdemo.service;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DatabaseServiceTest {
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