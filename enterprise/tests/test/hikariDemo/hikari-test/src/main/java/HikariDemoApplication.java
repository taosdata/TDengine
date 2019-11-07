import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HikariDemoApplication {

    public static void main(String[] args) {
        HikariConfig config = new HikariConfig("/home/jyhou/workspace/taosdata/test/hikariDemo/hikari-test/src/hikari.properties");
        System.out.println("DriverClassName: " + config.getDriverClassName());
        System.out.println("JdbcUrl: " + config.getJdbcUrl());
        System.out.println("ConnectionTimeout: " + config.getConnectionTimeout());
        System.out.println("MaximumPoolSize: " + config.getMaximumPoolSize());
        System.out.println("MinimumIdle: " + config.getMinimumIdle());
        System.out.println("Username: " + config.getUsername());
        System.out.println("Password: " + config.getPassword());
        HikariDataSource ds = new HikariDataSource(config);

        String db = "db";
        String table = "tb";

        int loop = 1000;
        int threads = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(loop);
        for (int i = 0; i < threads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    long count = 0l;
                    try {

                        String threadName = Thread.currentThread().getName();
                        for (int j =0; j < loop; j++) {
                            // here get connection
                            Connection connection = ds.getConnection();

                            System.out.printf("%s: connected to server\n", threadName);
                            Statement stmt = connection.createStatement();
                            stmt.executeUpdate("use " + db);
                            System.out.printf("%s: query: \"select count(*) from %s\"\n", threadName, table);
                            long start = System.nanoTime();
                            ResultSet res = stmt.executeQuery("select count(*) from " + table);
                            while (res.next()) {
                                for (int col = 1; col <= res.getMetaData().getColumnCount(); col++) {
                                    res.getObject(col);
                                    System.out.printf("%s", res.getObject(col).toString());
                                }
                                System.out.println("\n");
                            }
                            count++;
                            long end = System.nanoTime();
                            end = end - start;
                            BigDecimal time = BigDecimal.valueOf(end).divide(BigDecimal.valueOf(1e9)); // time used in seconds
                            System.out.printf("%s: Query completed.\n Repeated times: %d\n Time used: %fs\n", threadName, count, time);
                            res.close();
                            stmt.close();

                            // close connection, put it back to connection pool
                            connection.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println(e.getMessage());
                        System.out.println("failed to connect");
                    }
                }
            });
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait
        }

        System.out.println("Test completed!");
    }
}
