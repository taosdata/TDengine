package com.taosdata.example.jdbcTaosdemo.task;

import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import com.taosdata.example.jdbcTaosdemo.utils.ConnectionFactory;
import com.taosdata.example.jdbcTaosdemo.utils.SqlSpeller;
import com.taosdata.example.jdbcTaosdemo.utils.TimeStampUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

public class InsertTableTask implements Runnable {
    private static final Logger logger = Logger.getLogger(InsertTableTask.class);
    private static AtomicLong beginTimestamp = new AtomicLong(TimeStampUtil.datetimeToLong("2005-01-01 00:00:00.000"));

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
            Connection connection = ConnectionFactory.build(config);
            // iterate insert
            for (int j = 0; j < recordsNumber; j++) {
                long ts = beginTimestamp.getAndIncrement();
                // insert data into echo table
                for (int i = startIndex; i < startIndex + tableNumber; i++) {
                    String sql = SqlSpeller.insertOneRowSQL(config.getDbName(), config.getTbPrefix(), i + 1, ts);
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
