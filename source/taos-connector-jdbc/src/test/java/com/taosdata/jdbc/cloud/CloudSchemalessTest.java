package com.taosdata.jdbc.cloud;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

@Ignore
public class CloudSchemalessTest {
    String url = null;
    public static SchemalessWriter writer;
    public static Connection connection;
    final String dbName = "javatest";

    @Before
    public void before() throws SQLException {
        url = System.getenv("TDENGINE_CLOUD_URL");
        if (url == null || "".equals(url.trim())) {
            System.out.println("Environment variable for CloudTest not set properly");
            return;
        }
        connection = DriverManager.getConnection(url);
        writer = new SchemalessWriter(url, null, null, dbName);
    }

    @Test
    public void testLine() throws SQLException {
        if (url == null || "".equals(url.trim())) {
            return;
        }

        // given
        long curTime = System.currentTimeMillis();
        String[] lines = new String[]{
                "st,t1=3i64,t2=4f64,t3=\"t3\",ts=" + curTime + " c1=3i64,c3=L\"passit\",c2=false,c4=4f64 " + curTime};

        // when
        writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
        // then
        Statement statement = connection.createStatement();
        statement.executeUpdate("use " + dbName);
        ResultSet rs = statement.executeQuery("select * from javatest.st order by _ts DESC limit 1");
        Assert.assertNotNull(rs);
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue(metaData.getColumnCount() > 0);
        while (rs.next()) {
            Assert.assertEquals(curTime, rs.getLong("_ts"));
        }
        rs.close();
        statement.close();
    }
}
