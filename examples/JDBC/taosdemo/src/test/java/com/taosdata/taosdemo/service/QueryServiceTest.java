package com.taosdata.taosdemo.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryServiceTest {
    private static QueryService queryService;

    @Test
    public void areValidQueries() {

    }

    @Test
    public void generateSuperTableQueries() {
        String[] sqls = queryService.generateSuperTableQueries("restful_test");
        for (String sql : sqls) {
            System.out.println(sql);
        }
    }

    @Test
    public void querySuperTable() {
        String[] sqls = queryService.generateSuperTableQueries("restful_test");
        queryService.querySuperTable(sqls, 1000, 10, 10);
    }

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
        config.setUsername("root");
        config.setPassword("taosdata");
        HikariDataSource dataSource = new HikariDataSource(config);
        queryService = new QueryService(dataSource);
    }

}