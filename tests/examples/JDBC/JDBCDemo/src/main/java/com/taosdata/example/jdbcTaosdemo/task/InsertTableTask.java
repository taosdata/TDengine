package com.taosdata.example.jdbcTaosdemo.task;

import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import com.taosdata.example.jdbcTaosdemo.utils.ConnectionFactory;
import com.taosdata.example.jdbcTaosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class InsertTableTask implements Runnable {
    private static final Logger logger = Logger.getLogger(InsertTableTask.class);

    private final JdbcTaosdemoConfig config;
    private final int startTbIndex;
    private final int tableNumber;
    private final int recordsNumberPerTable;

    public InsertTableTask(JdbcTaosdemoConfig config, int startTbIndex, int tableNumber, int recordsNumberPerTable) {
        this.config = config;
        this.startTbIndex = startTbIndex;
        this.tableNumber = tableNumber;
        this.recordsNumberPerTable = recordsNumberPerTable;
    }

    @Override
    public void run() {
        try {
            Connection connection = ConnectionFactory.build(config);
            int keep = config.keep;
            Instant end = Instant.now();
            Instant start = end.minus(Duration.ofDays(keep - 1));
            long timeGap = ChronoUnit.MILLIS.between(start, end) / (recordsNumberPerTable - 1);

            // iterate insert
            for (int j = 0; j < recordsNumberPerTable; j++) {
                long ts = start.toEpochMilli() + (j * timeGap);
                // insert data into echo table
                for (int i = startTbIndex; i < startTbIndex + tableNumber; i++) {
                    String sql = SqlSpeller.insertBatchSizeRowsSQL(config.database, config.prefixOfTable, i + 1, ts, config.numOfValuesPerSQL);
                    logger.info(Thread.currentThread().getName() + ">>> " + sql);
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    statement.close();
                }
            }
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
