package com.taosdata.jdbc.cases;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.Properties;

public class CalciteTest {

    public static void main(String[] args) throws SqlParseException, ClassNotFoundException, SQLException {

//        CalciteConnection calciteConnection = testMyqsl();
        CalciteConnection calciteConnection = testTSDB();

        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test.t");

        while (resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                System.out.println(columnLabel + " : " + resultSet.getString(i));
            }
        }
        resultSet.close();
        statement.close();
        calciteConnection.close();
    }

    private static CalciteConnection testMyqsl() throws ClassNotFoundException, SQLException {
        //创建Calcite Connection对象
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // JDBC adapter
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://192.168.236.135:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        JdbcSchema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);

        return calciteConnection;
    }

    private static CalciteConnection testTSDB() throws SQLException, ClassNotFoundException {
        //创建Calcite Connection对象
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("caseSensitive", "false");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // JDBC adapter
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:TAOS://192.168.236.135:6030/test");
        dataSource.setUsername("root");
        dataSource.setPassword("taosdata");
        JdbcSchema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);

        return calciteConnection;
    }
}
