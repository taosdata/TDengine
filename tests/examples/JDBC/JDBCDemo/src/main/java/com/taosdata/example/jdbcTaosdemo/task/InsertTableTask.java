package com.taosdata.example.jdbcTaosdemo.task;

import com.taosdata.example.jdbcTaosdemo.JdbcTaosdemo;
import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertTableTask implements Runnable {
    private static final Logger logger = Logger.getLogger(InsertTableTask.class);

    private final JdbcTaosdemoConfig config;
    private final int startIndex;
    private final int tableNumber;
    private final int recordsNumber;

    public InsertTableTask(JdbcTaosdemoConfig config, int startIndex, int tableNumber, int recordsNumber) {
        this.config = config;
        this.startIndex = startIndex;
        this.tableNumber = tableNumber;
        this.recordsNumber = recordsNumber;
    }

    @Override
    public void run() {
        try {
            Connection connection = JdbcTaosdemo.getConnection(config);
            for (int i = startIndex; i < startIndex + tableNumber; i++) {
                for (int j = 0; j < recordsNumber; j++) {
                    String sql = JdbcTaosdemo.insertSql(i + 1, config);
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    statement.close();
                    logger.info(Thread.currentThread().getName() + ">>> " + sql);
                }
            }
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
