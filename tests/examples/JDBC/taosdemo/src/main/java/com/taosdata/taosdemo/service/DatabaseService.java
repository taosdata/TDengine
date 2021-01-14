package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.dao.DatabaseMapper;
import com.taosdata.taosdemo.dao.DatabaseMapperImpl;

import javax.sql.DataSource;
import java.util.Map;

public class DatabaseService {

    private final DatabaseMapper databaseMapper;

    public DatabaseService(DataSource dataSource) {
        this.databaseMapper = new DatabaseMapperImpl(dataSource);
    }

    // 建库，指定 name
    public void createDatabase(String database) {
        databaseMapper.createDatabase(database);
    }

    // 建库，指定参数 keep,days,replica等
    public void createDatabase(Map<String, String> map) {
        if (map.isEmpty())
            return;
        if (map.containsKey("database") && map.size() == 1) {
            createDatabase(map.get("database"));
            return;
        }
        databaseMapper.createDatabaseWithParameters(map);
    }

    // drop database
    public void dropDatabase(String dbname) {
        databaseMapper.dropDatabase(dbname);
    }

    // use database
    public void useDatabase(String dbname) {
        databaseMapper.useDatabase(dbname);
    }
}
