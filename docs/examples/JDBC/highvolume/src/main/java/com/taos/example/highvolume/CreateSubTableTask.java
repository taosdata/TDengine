package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class CreateSubTableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(CreateSubTableTask.class);
    private final int taskId;
    private final int subTableStartIndex;
    private final int subTableEndIndex;
    private final String dbName;

    public CreateSubTableTask(int taskId,
                              int subTableStartIndex,
                              int subTableEndIndex,
                              String dbName) {
        this.taskId = taskId;
        this.subTableStartIndex = subTableStartIndex;
        this.subTableEndIndex = subTableEndIndex;
        this.dbName = dbName;
    }

    @Override
    public void run() {
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()){
            statement.execute("use " + dbName);
            StringBuilder sql = new StringBuilder();
            sql.append("create table");
            int i = 0;
            for (int tableNum = subTableStartIndex; tableNum <= subTableEndIndex; tableNum++) {
                sql.append(" if not exists " + Util.getTableNamePrefix() + tableNum + " using meters" + " tags(" + tableNum + ", " + "\"location_" + tableNum + "\"" + ")");

                if (i < 1000) {
                    i++;
                } else {
                    statement.execute(sql.toString());
                    sql = new StringBuilder();
                    sql.append("create table");
                    i = 0;
                }
            }
            if (sql.length() > "create table".length()) {
                statement.execute(sql.toString());
            }
        } catch (SQLException e) {
            logger.error("task id {}, failed to create sub table", taskId, e);
        }
    }


}