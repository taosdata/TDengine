package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class AbstractConnectionTest {
    private AbstractConnection connection;

    @Before
    public void setUp() {
        connection = new AbstractConnection(new Properties(), TSDBConstants.UNKNOWN_VERSION) {
            @Override
            public Statement createStatement() throws SQLException {
                return null; // Mock implementation
            }

            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                return null; // Mock implementation
            }

            @Override
            public void close() throws SQLException {
                // Mock implementation
                isClosed = true;
            }

            @Override
            public boolean isClosed() throws SQLException {
                return isClosed; // Mock implementation
            }

            @Override
            public DatabaseMetaData getMetaData() throws SQLException {
                return null; // Mock implementation
            }

            @Override
            public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException{
                // do nothing
            }

            @Override
            public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException{
                return 0;
            }

        };
    }

    @Test(expected = SQLException.class)
    public void testNativeSQLWhenClosed() throws SQLException {
        connection.close();
        connection.nativeSQL("SELECT * FROM table");
    }

    @Test(expected = SQLException.class)
    public void testSetAutoCommitWhenClosed() throws SQLException {
        connection.close();
        connection.setAutoCommit(true);
    }

    @Test(expected = SQLException.class)
    public void testGetAutoCommitWhenClosed() throws SQLException {
        connection.close();
        connection.getAutoCommit();
    }

    @Test(expected = SQLException.class)
    public void testCommitWhenClosed() throws SQLException {
        connection.close();
        connection.commit();
    }

    @Test(expected = SQLException.class)
    public void testRollbackWhenClosed() throws SQLException {
        connection.close();
        connection.rollback();
    }

    @Test(expected = SQLException.class)
    public void testSetReadOnlyWhenClosed() throws SQLException {
        connection.close();
        connection.setReadOnly(true);
    }

    @Test(expected = SQLException.class)
    public void testIsReadOnlyWhenClosed() throws SQLException {
        connection.close();
        connection.isReadOnly();
    }

    @Test(expected = SQLException.class)
    public void testSetCatalogWhenClosed() throws SQLException {
        connection.close();
        connection.setCatalog("testCatalog");
    }

    @Test(expected = SQLException.class)
    public void testGetCatalogWhenClosed() throws SQLException {
        connection.close();
        connection.getCatalog();
    }

    @Test(expected = SQLException.class)
    public void testSetTransactionIsolationWhenClosed() throws SQLException {
        connection.close();
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test(expected = SQLException.class)
    public void testGetTransactionIsolationWhenClosed() throws SQLException {
        connection.close();
        connection.getTransactionIsolation();
    }

    @Test(expected = SQLException.class)
    public void testClearWarningsWhenClosed() throws SQLException {
        connection.close();
        connection.clearWarnings();
    }

    @Test(expected = SQLException.class)
    public void testGetWarningsWhenClosed() throws SQLException {
        connection.close();
        connection.getWarnings();
    }

    @Test(expected = SQLException.class)
    public void testSetHoldabilityWhenClosed() throws SQLException {
        connection.close();
        connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLException.class)
    public void testGetHoldabilityWhenClosed() throws SQLException {
        connection.close();
        connection.getHoldability();
    }

    @Test(expected = SQLException.class)
    public void testSetSchemaWhenClosed() throws SQLException {
        connection.close();
        connection.setSchema("testSchema");
    }

    @Test(expected = SQLException.class)
    public void testGetSchemaWhenClosed() throws SQLException {
        connection.close();
        connection.getSchema();
    }

    @Test(expected = SQLException.class)
    public void testAbortWhenClosed() throws SQLException {
        connection.close();
        connection.abort(null);
    }

    @Test(expected = SQLException.class)
    public void testSetNetworkTimeoutWhenClosed() throws SQLException {
        connection.close();
        connection.setNetworkTimeout(null, 1000);
    }

    @Test(expected = SQLException.class)
    public void testGetNetworkTimeoutWhenClosed() throws SQLException {
        connection.close();
        connection.getNetworkTimeout();
    }

    @Test(expected = SQLException.class)
    public void testCreateStatement() throws SQLException {
        connection.createStatement(999, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLException.class)
    public void testCreateStatement2() throws SQLException {
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, 999);
    }

    @Test
    public void testCreateStatement3() throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertNull(statement);
    }

    @Test(expected = SQLException.class)
    public void testCreateStatement4() throws SQLException {
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    }

    @Test(expected = SQLException.class)
    public void testCreateStatement5() throws SQLException {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }
    @Test(expected = SQLException.class)
    public void testCreateStatement6() throws SQLException {
        connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLException.class)
    public void testCreateStatement7() throws SQLException {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLException.class)
    public void testCreatePrepareStatement() throws SQLException {
        connection.prepareStatement("", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    public void testSetReadOnly() throws SQLException {
        connection.setReadOnly(false);
        assertTrue(connection.isReadOnly());
    }
    @Test
    public void testIsReadOnly() throws SQLException {
        assertTrue(connection.isReadOnly());
    }

    @Test
    public void testSetCatalog() throws SQLException {
        connection.setCatalog("testCatalog");
        // 这里可以添加验证逻辑，确保目录被正确设置
    }
    @Test
    public void testGetCatalog() throws SQLException {
        connection.setCatalog("testCatalog");
        assertEquals("testCatalog", connection.getCatalog());
    }

    @Test(expected = SQLException.class)
    public void testSetTransactionIsolation_ConnectionClosed() throws SQLException {
        connection.close();
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

    @Test(expected = SQLException.class)
    public void testSetTransactionIsolation_InvalidLevel() throws SQLException {
        connection.setTransactionIsolation(999); // 无效的隔离级别
    }

    @Test
    public void testSetTransactionIsolation() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        Assert.assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
    }
    @Test (expected = SQLException.class)
    public void testSetTransactionIsolation2() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }
    @Test (expected = SQLException.class)
    public void testSetTransactionIsolation3() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    @Test (expected = SQLException.class)
    public void testSetTransactionIsolation4() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }
    @Test (expected = SQLException.class)
    public void testSetTransactionIsolation5() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Test
    public void testGetTransactionIsolation() throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
    }

    @Test
    public void testGetWarnings() throws SQLException {
        SQLWarning warning = connection.getWarnings();
        assertNull(warning); // 假设没有警告
    }
    @Test
    public void testClearWarnings() throws SQLException {
        connection.clearWarnings();
        Assert.assertNotNull(connection);
    }

    @Test(expected = SQLException.class)
    public void testCreateClob_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createClob();
    }

    @Test(expected = SQLException.class)
    public void testCreateClob_UnsupportedMethod() throws SQLException {
        connection.createClob();
    }

    @Test(expected = SQLException.class)
    public void testCreateBlob_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createBlob();
    }

    @Test(expected = SQLException.class)
    public void testCreateBlob_UnsupportedMethod() throws SQLException {
        connection.createBlob();
    }

    @Test(expected = SQLException.class)
    public void testCreateNClob_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createNClob();
    }

    @Test(expected = SQLException.class)
    public void testCreateNClob_UnsupportedMethod() throws SQLException {
        connection.createNClob();
    }

    @Test(expected = SQLException.class)
    public void testCreateSQLXML_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createSQLXML();
    }

    @Test(expected = SQLException.class)
    public void testCreateSQLXML_UnsupportedMethod() throws SQLException {
        connection.createSQLXML();
    }

    @Test(expected = SQLException.class)
    public void testGetClientInfo_ConnectionClosed() throws SQLException {
        connection.close();
        connection.getClientInfo("testName");
    }

    @Test
    public void testGetClientInfo() throws SQLException {
        connection.clientInfoProps.setProperty("testName", "testValue");
        assertEquals("testValue", connection.getClientInfo("testName"));
    }

    @Test(expected = SQLException.class)
    public void testGetClientInfoAll_ConnectionClosed() throws SQLException {
        connection.close();
        connection.getClientInfo();
    }

    @Test
    public void testGetClientInfoAll() throws SQLException {
        connection.clientInfoProps.setProperty("testName", "testValue");
        Properties props = connection.getClientInfo();
        assertEquals("testValue", props.getProperty("testName"));
    }

    @Test(expected = SQLException.class)
    public void testCreateArrayOf_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createArrayOf("typeName", new Object[]{});
    }

    @Test(expected = SQLException.class)
    public void testCreateArrayOf_UnsupportedMethod() throws SQLException {
        connection.createArrayOf("typeName", new Object[]{});
    }

    @Test(expected = SQLException.class)
    public void testCreateStruct_ConnectionClosed() throws SQLException {
        connection.close();
        connection.createStruct("typeName", new Object[]{});
    }

    @Test(expected = SQLException.class)
    public void testCreateStruct_UnsupportedMethod() throws SQLException {
        connection.createStruct("typeName", new Object[]{});
    }

    @Test
    public void testSetSchema() throws SQLException {
        connection.setSchema("testSchema");
        Assert.assertNull(connection.getSchema());
    }

    @Test
    public void testGetSchema() throws SQLException {
        assertNull(connection.getSchema()); // 假设返回 null
    }
    @Test
    public void testAbort() throws SQLException {
        connection.abort(null);
        // 这里可以添加验证逻辑，确保没有异常抛出
    }

    @Test(expected = SQLException.class)
    public void testSetNetworkTimeout_InvalidTimeout() throws SQLException {
        connection.setNetworkTimeout(null, -1); // 无效的超时值
    }

    @Test
    public void testSetNetworkTimeout() throws SQLException {
        connection.setNetworkTimeout(null, 1000);
        // 这里可以添加验证逻辑，确保没有异常抛出
    }

    @Test
    public void testGetNetworkTimeout() throws SQLException {
        assertEquals(0, connection.getNetworkTimeout()); // 假设默认返回 0
    }

}