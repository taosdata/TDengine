package com.taosdata.example.jdbcTaosdemo.task;

import com.taosdata.example.jdbcTaosdemo.JdbcTaosdemo;
import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertTableDatetimeTask implements Runnable {
    private static Logger logger = Logger.getLogger(InsertTableDatetimeTask.class);

    private final JdbcTaosdemoConfig config;
    private final int startTableIndex;
    private final int tableNumber;
    private final long startDatetime;
    private final long finishedDatetime;

    public InsertTableDatetimeTask(JdbcTaosdemoConfig config, int startTableIndex, int tableNumber, long startDatetime, long finishedDatetime) {
        this.config = config;
        this.startTableIndex = startTableIndex;
        this.tableNumber = tableNumber;
        this.startDatetime = startDatetime;
        this.finishedDatetime = finishedDatetime;
    }

    @Override
    public void run() {
        try {
            Connection connection = JdbcTaosdemo.getConnection(config);
            int valueCnt = 100;
            for (long ts = startDatetime; ts < finishedDatetime; ts+= valueCnt) {
                for (int i = startTableIndex; i < startTableIndex + tableNumber; i++) {
//                    String sql = JdbcTaosdemo.insertSql(i + 1, ts, config);

                    String sql = JdbcTaosdemo.batchInsertSql(i + 1, ts, valueCnt, config);
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
