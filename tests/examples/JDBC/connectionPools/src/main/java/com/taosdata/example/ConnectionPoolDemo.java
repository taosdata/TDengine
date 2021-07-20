package com.taosdata.example;

import com.taosdata.example.common.InsertTask;
import com.taosdata.example.pool.C3p0Builder;
import com.taosdata.example.pool.DbcpBuilder;
import com.taosdata.example.pool.DruidPoolBuilder;
import com.taosdata.example.pool.HikariCpBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConnectionPoolDemo {

    private static Logger logger = LogManager.getLogger(DruidPoolBuilder.class);
    private static final String dbName = "pool_test";

    private static String poolType = "hikari";
    private static long totalSize = 1_000_000l;
    private static long tableSize = 1;
    private static long batchSize = 1;

    private static int poolSize = 50;
    private static int threadCount = 50;
    private static int sleep = 0;

    public static void main(String[] args) {
        String host = null;
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-poolType".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                poolType = args[++i];
            }
            if ("-recordNumber".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                totalSize = Long.parseLong(args[++i]);
            }
            if ("-tableNumber".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                tableSize = Long.parseLong(args[++i]);
            }
            if ("-batchNumber".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                batchSize = Long.parseLong(args[++i]);
            }

        }
        if (host == null) {
            System.out.println("Usage: java -jar XXX.jar -host <hostname> " +
                    "-poolType <c3p0| dbcp| druid| hikari>" +
                    "-recordNumber <number> " +
                    "-tableNumber <number> " +
                    "-batchNumber <number> " +
                    "-sleep <number> "
            );
            return;
        }

        DataSource dataSource;
        switch (poolType) {
            case "c3p0":
                dataSource = C3p0Builder.getDataSource(host, poolSize);
                break;
            case "dbcp":
                dataSource = DbcpBuilder.getDataSource(host, poolSize);
                break;
            case "druid":
                dataSource = DruidPoolBuilder.getDataSource(host, poolSize);
                break;
            case "hikari":
            default:
                dataSource = HikariCpBuilder.getDataSource(host, poolSize);
                poolType = "hikari";
        }

        logger.info(">>>>>>>>>>>>>> connection pool Type: " + poolType);
        init(dataSource);

//        try {
//            Connection connection = dataSource.getConnection();
//            Statement statement = connection.createStatement();
//            String sql = "insert into " + dbName + ".t_1 values('2020-01-01 00:00:00.000',12.12,111)";
//            int affectRows = statement.executeUpdate(sql);
//            System.out.println("affectRows >>> " + affectRows);
//            affectRows = statement.executeUpdate(sql);
//            System.out.println("affectRows >>> " + affectRows);
//            statement.close();
//            connection.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (long i = 0; i < totalSize / tableSize / batchSize; i++) {
            executor.execute(new InsertTask(dataSource, dbName, tableSize, batchSize));
            // sleep few seconds
            try {
                if (sleep > 0)
                    TimeUnit.MILLISECONDS.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();

    }

    private static void init(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            execute(conn, "drop database if exists " + dbName + "");
            execute(conn, "create database if not exists " + dbName + "");
            execute(conn, "use " + dbName + "");
            execute(conn, "create table weather(ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)");
            for (int tb_ind = 1; tb_ind <= tableSize; tb_ind++) {
                execute(conn, "create table t_" + tb_ind + " using weather tags('beijing'," + (tb_ind + 1) + ")");
            }
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> init finished.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void execute(Connection con, String sql) {
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            logger.info("SQL >>> " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
