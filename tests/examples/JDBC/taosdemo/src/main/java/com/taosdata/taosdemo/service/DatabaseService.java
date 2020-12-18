package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.mapper.DatabaseMapper;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Service
public class DatabaseService {

    @Autowired
    private DatabaseMapper databaseMapper;

    private DataSource dataSource;
    private static Logger logger = Logger.getLogger(DatabaseService.class);

    public DatabaseService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // 建库，指定 name
    public int createDatabase(String database) {
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("create database " + database);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
//        return databaseMapper.createDatabase(database);
    }

    // 建库，指定参数 keep,days,replica等
    public int createDatabase(Map<String, String> map) {
        if (map.isEmpty())
            return 0;
        if (map.containsKey("database") && map.size() == 1)
            createDatabase(map.get("database"));
//            return databaseMapper.createDatabase(map.get("database"));
//        return databaseMapper.createDatabaseWithParameters(map);
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = "create database if not exists " + map.get("database")
                    + " keep " + map.get("keep")
                    + " days " + map.get("days")
                    + " replica " + map.get("replica");
            logger.info(">>> " + sql);
            statement.execute(sql);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    // drop database
    public int dropDatabase(String dbname) {
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = "drop database if exists " + dbname;
            logger.info(">>> " + sql);
            statement.execute(sql);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    // use database
    public int useDatabase(String dbname) {
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = "use " + dbname;
            logger.info(">>> " + sql);
            statement.execute(sql);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
//        return databaseMapper.useDatabase(dbname);
    }
}
