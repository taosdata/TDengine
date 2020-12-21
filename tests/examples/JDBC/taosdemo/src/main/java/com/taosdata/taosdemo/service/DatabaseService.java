package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.mapper.DatabaseMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DatabaseService {

    @Autowired
    private DatabaseMapper databaseMapper;

    // 建库，指定 name
    public int createDatabase(String database) {
        return databaseMapper.createDatabase(database);
    }

    // 建库，指定参数 keep,days,replica等
    public int createDatabase(Map<String, String> map) {
        if (map.isEmpty())
            return 0;
        if (map.containsKey("database") && map.size() == 1)
            return databaseMapper.createDatabase(map.get("database"));
        return databaseMapper.createDatabaseWithParameters(map);
    }

    // drop database
    public int dropDatabase(String dbname) {
        return databaseMapper.dropDatabase(dbname);
    }

    // use database
    public int useDatabase(String dbname) {
        return databaseMapper.useDatabase(dbname);
    }
}
