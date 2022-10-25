package com.taosdata.taosdemo.service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryService {

    private final DataSource dataSource;

    public QueryService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /* only select or show SQL Statement is valid for executeQuery */
    public Boolean[] areValidQueries(String[] sqls) {
        Boolean[] ret = new Boolean[sqls.length];
        for (int i = 0; i < sqls.length; i++) {
            ret[i] = true;
            try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeQuery(sqls[i]);
            } catch (SQLException e) {
                ret[i] = false;
                continue;
            }
        }
        return ret;
    }

    public String[] generateSuperTableQueries(String dbName) {
        List<String> sqls = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("use " + dbName);
            ResultSet rs = stmt.executeQuery("show stables");
            while (rs.next()) {
                String name = rs.getString("name");
                sqls.add("select count(*) from " + dbName + "." + name);
                sqls.add("select first(*) from " + dbName + "." + name);
                sqls.add("select last(*) from " + dbName + "." + name);
                sqls.add("select last_row(*) from " + dbName + "." + name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String[] sqlArr = new String[sqls.size()];
        return sqls.toArray(sqlArr);
    }

    public void querySuperTable(String[] sqls, int interval, int threadCount, long queryTimes) {
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(() -> {
            // do query
            try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
                long count = queryTimes;
                if (count == 0)
                    count = Long.MAX_VALUE;
                while (count > 0) {
                    for (String sql : sqls) {
                        long start = System.currentTimeMillis();
                        ResultSet rs = stmt.executeQuery(sql);
                        printResultSet(rs);
                        long end = System.currentTimeMillis();
                        long timecost = end - start;
                        if (interval - timecost > 0) {
                            TimeUnit.MILLISECONDS.sleep(interval - timecost);
                        }
                    }
                    count--;
                }

            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }

        })).collect(Collectors.toList());
        threads.stream().forEach(Thread::start);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
            }
            System.out.println();
        }
    }
}
