package com.taosdata.example.jdbcTaosdemo.task;

import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import com.taosdata.example.jdbcTaosdemo.utils.ConnectionFactory;
import com.taosdata.example.jdbcTaosdemo.utils.SqlSpeller;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTableTask implements Runnable {

    private static Logger logger = Logger.getLogger(CreateTableTask.class);
    private final JdbcTaosdemoConfig config;
    private final int startIndex;
    private final int tableNumber;

    public CreateTableTask(JdbcTaosdemoConfig config, int startIndex, int tableNumber) {
        this.config = config;
        this.startIndex = startIndex;
        this.tableNumber = tableNumber;
    }

    @Override
    public void run() {
        try {
            Connection connection = ConnectionFactory.build(config);
            for (int i = startIndex; i < startIndex + tableNumber; i++) {
                Statement statement = connection.createStatement();
                String sql = SqlSpeller.createTableSQL(i + 1, config.database, config.superTable);
                statement.execute(sql);
                statement.close();
                logger.info(">>> " + sql);
            }
            connection.close();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
