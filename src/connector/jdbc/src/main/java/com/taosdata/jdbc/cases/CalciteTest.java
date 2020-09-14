package com.taosdata.jdbc.cases;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CalciteTest {

    public static void main(String[] args) throws SqlParseException, ClassNotFoundException, SQLException {

        //创建Calcite Connection对象
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // JDBC adapter
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://192.168.56.101:3306");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        Map<String, String> map = new HashMap<>();
        JdbcSchema schema = JdbcSchema.create(rootSchema, "hr", dataSource, null, null);
        rootSchema.add("hr", schema);

        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from hr.depts");

        while (resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                System.out.println(columnLabel + " : " + resultSet.getString(i));
            }
        }
        resultSet.close();
        statement.close();
        connection.close();


        //创建TDengine的数据源schema
//        Class.forName("com.taosdata.jdbc.TSDBDriver");
//        String url = "jdbc:TAOS://127.0.0.1:6030/hdb";
//        dataSource.setUrl(url);
//        dataSource.setUsername("root");
//        dataSource.setPassword("taosdata");

//        Class.forName("com.mysql.jdbc.Driver");
//        String url = "jdbc:mysql://localhost:3306/hdb";
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setUrl(url);
//        dataSource.setUsername("root");
//        dataSource.setPassword("123456");
        //这里hdb是在tdengine中创建的数据库名
//        JdbcSchema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
//        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, "hdb", null);
        //创建新的schema自动映射到原来的hdb数据库
//        rootSchema.add("test", schema);

//        Statement stmt = calciteConnection.createStatement();
        //查询schema test中的表，表名是tdengine中的表
//        ResultSet rs = stmt.executeQuery("select * from test.t");
//        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
//            System.out.println(rs.getMetaData().getColumnName(i));
//        }
//        while (rs.next()) {
//            System.out.println(rs.getObject(1));
//        }
    }
}
