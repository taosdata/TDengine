package com.taosdata.taosdemo.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DatabaseServiceTest {

    private static DatabaseService service;

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

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
        config.setUsername("root");
        config.setPassword("taosdata");
        HikariDataSource dataSource = new HikariDataSource(config);
        service = new DatabaseService(dataSource);
    }

}