package com.taosdata.example.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.Properties;

public class CalciteDemo {

    private static String url_taos = "jdbc:TAOS://192.168.236.135:6030/test";
    private static String url_mysql = "jdbc:mysql://master:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF-8";

    public static void main(String[] args) throws SqlParseException, ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("caseSensitive", "false");

        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        //这里hdb是在tdengine中创建的数据库名
        Schema schema = mysqlTest(rootSchema);
//        Schema schema = tdengineTest(rootSchema);

        //创建新的schema自动映射到原来的hdb数据库
        rootSchema.add("test", schema);

        Statement stmt = calciteConnection.createStatement();
        //查询schema test中的表，表名是tdengine中的表
        ResultSet rs = stmt.executeQuery("select * from test.t");
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.println(metaData.getColumnLabel(i) + " : " + rs.getString(i));
            }
        }
    }


    private static Schema tdengineTest(SchemaPlus rootSchema) throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url_taos);
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");

        return JdbcSchema.create(rootSchema, "test", dataSource, "hdb", null);
    }

    private static Schema mysqlTest(SchemaPlus rootSchema) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url_mysql);
        dataSource.setUsername("root");
        dataSource.setPassword("123456");

        //Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, "hdb", null);
        return JdbcSchema.create(rootSchema, "test", dataSource, "test", null);
    }
}
