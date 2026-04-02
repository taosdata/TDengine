package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

public class QueryTest {

    private static String host = "192.168.1.113";
    private static String db = "dbcmp";
    private static String user = "root";
    private static String password = "taosdata";
    private static String port = "0";
    private static String jdbcProtocal = "";
    private static String cfgDir = "D:\\taos-1.4.14-windows-client-x64-20181224-120254\\cfg";
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String TSDB_URL = "jdbc:TAOS://192.168.1.113:0/?user=root&password=taosdata";
    private Connection connection;



    public static void main(String[] args) throws SQLException {
        QueryTest  queryTest= new QueryTest();
//            jdbcConnectionTest.setDb(db);
//            jdbcConnectionTest.setHost("192.168.1.113");
//            jdbcConnectionTest.setCfgDir("D:\\taos-1.4.14-windows-client-x64-20181224-120254\\cfg");
//        jdbcConnectionTest.getConnection();
        Connection connection = null;
        try {
            Class.forName(TSDB_DRIVER);
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, cfgDir);
            connection = DriverManager.getConnection(TSDB_URL, properties);
//            System.out.println("connected to server");
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("use " + db);
            for (int i = 0; i < 100; i++) {
                queryTest.singleTableQuery(connection);
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        System.out.println("Finished.");

    }

    private void singleTableQuery(Connection connection) throws SQLException{
        long count = 0L;
        Statement stmt = connection.createStatement();
        String sql = "select measure1 from device1 where ts >= '2018-01-01 00:00:00.000' and ts <= '2018-01-01 10:00:00.000'";
//        System.out.println("query: " + sql);
        long start = System.nanoTime();
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
//            for (int col = 1; col <= res.getMetaData().getColumnCount(); col++) {
//                res.getObject(col);
//            }
            count++;
        }
//        if (count != 86400) {
//            System.out.println("count=" + count);
//            throw new RuntimeException("Number of rows fetched is not 864000!");
//        }
        long end = System.nanoTime();
        end = end - start;
//            BigDecimal time = BigDecimal.valueOf(end).divide(BigDecimal.valueOf(1e9)); // time used in seconds
//            System.out.printf("Query completed.\n Number of rows retrieved: %d\n Time used: %fs\n", count, time);
        System.out.println(end);
        res.close();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getJdbcProtocal() {
        return jdbcProtocal;
    }

    public void setJdbcProtocal(String jdbcProtocal) {
        this.jdbcProtocal = jdbcProtocal;
    }

    public String getCfgDir() {
        return cfgDir;
    }

    public void setCfgDir(String cfgDir) {
        this.cfgDir = cfgDir;
    }

}
