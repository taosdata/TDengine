package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class StatTask implements Runnable, Stoppable {
    private final static Logger logger = LoggerFactory.getLogger(StatTask.class);
    private final int subTableNum;
    private final String dbName;
    private final Connection conn;
    private final Statement stmt;
    private volatile boolean  active = true;


    public StatTask(String dbName,
                    int subTableNum) throws SQLException {
        this.dbName = dbName;
        this.subTableNum = subTableNum;
        this.conn = Util.getConnection();
        this.stmt = conn.createStatement();
    }

    @Override
    public void run() {
        long lastCount = 0;

        while (active) {
            try {
                Thread.sleep(10000);

                long count = Util.count(stmt, dbName);
                logger.info("numberOfTable={} count={} speed={}", subTableNum, count, (count - lastCount) / 10);
                lastCount = count;
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
                break;
            } catch (SQLException e) {
                logger.error("execute sql error: ", e);
                break;
            }
        }

        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error("close connection error: ", e);
        }
    }

    public void stop() {
        active = false;
    }


}