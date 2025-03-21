package com.taos.example.highvolume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

class StatTask implements Runnable, Stoppable {
    private final static Logger logger = LoggerFactory.getLogger(StatTask.class);
    private final DataBaseMonitor databaseMonitor;
    private final int subTableNum;
    private volatile boolean  active = true;


    public StatTask(String dbName,
                    int subTableNum) throws SQLException {
        this.databaseMonitor = new DataBaseMonitor().init(dbName);
        this.subTableNum = subTableNum;
    }

    @Override
    public void run() {
        long lastCount = 0;
        while (active) {
            try {
                Thread.sleep(10000);

                long count = databaseMonitor.count();
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

        databaseMonitor.close();
    }

    public void stop() {
        active = false;
    }
}