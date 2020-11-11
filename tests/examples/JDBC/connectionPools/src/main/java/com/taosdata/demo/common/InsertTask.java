package com.taosdata.demo.common;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class InsertTask implements Runnable {
    private final Random random = new Random(System.currentTimeMillis());
    private static Logger logger = Logger.getLogger(InsertTask.class);

    private final DataSource ds;
    private final int batchSize;
    private final String dbName;
    private final int tableSize;

    public InsertTask(DataSource ds, String dbName, int tableSize, int batchSize) {
        this.ds = ds;
        this.dbName = dbName;
        this.tableSize = tableSize;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        Connection conn = null;
        Statement stmt = null;
        int affectedRows = 0;

        long start = System.currentTimeMillis();
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement();

            for (int tb_index = 1; tb_index <= tableSize; tb_index++) {
                StringBuilder sb = new StringBuilder();
                sb.append("insert into " + dbName + ".t_" + tb_index + "(ts, temperature, humidity) values ");
                for (int i = 0; i < batchSize; i++) {
                    sb.append("(" + (start + i) + ", " + (random.nextFloat() * 30) + ", " + (random.nextInt(70)) + ") ");
                }
                logger.info("SQL >>> " + sb.toString());
                affectedRows += stmt.executeUpdate(sb.toString());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            logger.info(">>> affectedRows:" + affectedRows + "  TimeCost:" + (System.currentTimeMillis() - start) + " ms");
        }
    }
}
