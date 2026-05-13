package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.utils.Printer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlExecuteTask implements Runnable {
    private final DataSource dataSource;
    private final String sql;

    public SqlExecuteTask(DataSource dataSource, String sql) {
        this.dataSource = dataSource;
        this.sql = sql;
    }

    @Override
    public void run() {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = stmt.execute(sql);
            long end = System.currentTimeMillis();
            if (execute) {
                ResultSet rs = stmt.getResultSet();
                Printer.printResult(rs);
            } else {
                Printer.printSql(sql, true, (end - start));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
