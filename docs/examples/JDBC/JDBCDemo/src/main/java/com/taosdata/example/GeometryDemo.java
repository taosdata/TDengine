package com.taosdata.example;

import com.taosdata.jdbc.TSDBPreparedStatement;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class GeometryDemo {
    private static String host = "localhost";
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private static final String user = "root";
    private static final String password = "taosdata";

    private Connection connection;

    public static void main(String[] args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1)
                host = args[++i];
        }
        if (host == null) {
            printHelp();
        }
        GeometryDemo demo = new GeometryDemo();
        demo.init();
        demo.createDatabase();
        demo.useDatabase();
        demo.dropTable();
        demo.createTable();

        demo.insert();
        demo.stmtInsert();
        demo.select();

        demo.dropTable();
        demo.close();
    }

    private void init() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;
        // get connection
        try {
            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection(url, properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createDatabase() {
        String sql = "create database if not exists " + dbName;
        execute(sql);
    }

    private void useDatabase() {
        String sql = "use " + dbName;
        execute(sql);
    }

    private void dropTable() {
        final String sql = "drop table if exists " + dbName + "." + tbName + "";
        execute(sql);
    }

    private void createTable() {
        final String sql = "create table if not exists " + dbName + "." + tbName + " (ts timestamp, temperature float, humidity int, location geometry(50))";
        execute(sql);
    }

    private void insert() {
        final String sql = "insert into " + dbName + "." + tbName + " (ts, temperature, humidity, location) values(now, 20.5, 34, 'POINT(1 2)')";
        execute(sql);
    }

    private void stmtInsert() throws SQLException {
        TSDBPreparedStatement preparedStatement = (TSDBPreparedStatement) connection.prepareStatement("insert into " + dbName + "." + tbName + " values (?, ?, ?, ?)");

        long current = System.currentTimeMillis();
        ArrayList<Long> tsList = new ArrayList<>();
        tsList.add(current);
        tsList.add(current + 1);
        preparedStatement.setTimestamp(0, tsList);
        ArrayList<Float> tempList = new ArrayList<>();
        tempList.add(20.1F);
        tempList.add(21.2F);
        preparedStatement.setFloat(1, tempList);
        ArrayList<Integer> humList = new ArrayList<>();
        humList.add(30);
        humList.add(31);
        preparedStatement.setInt(2, humList);


        ArrayList<byte[]> list = new ArrayList<>();
        GeometryFactory gf = new GeometryFactory();
        Point p1 = gf.createPoint(new Coordinate(1,2));
        p1.setSRID(1234);

        // NOTE: TDengine current version only support 2D dimension and little endian byte order
        WKBWriter w = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN, true);
        byte[] wkb = w.write(p1);
        list.add(wkb);

        Coordinate[] coordinates = { new Coordinate(10, 20),
                                     new Coordinate(30, 40)};
        LineString lineString = gf.createLineString(coordinates);
        lineString.setSRID(2345);
        byte[] wkb2 = w.write(lineString);
        list.add(wkb2);

        preparedStatement.setGeometry(3, list, 50);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
    }

    private void select() {
        final String sql = "select * from " + dbName + "." + tbName;
        executeQuery(sql);
    }

    private void close() {
        try {
            if (connection != null) {
                this.connection.close();
                System.out.println("connection closed.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void executeQuery(String sql) {
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));

            while (resultSet.next()){
                byte[] result1 = resultSet.getBytes(4);
                WKBReader reader = new WKBReader();
                Geometry g1 = reader.read(result1);
                System.out.println("GEO OBJ: " + g1 + ", SRID: " + g1.getSRID());
            }

        } catch (SQLException e) {
            long end = System.currentTimeMillis();
            printSql(sql, false, (end - start));
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void execute(String sql) {
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
        } catch (SQLException e) {
            long end = System.currentTimeMillis();
            printSql(sql, false, (end - start));
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JDBCDemo.jar -host <hostname>");
        System.exit(0);
    }

}
