package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.entity.ConCheckInfo;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
    * JUnit4 Test class to verify the lock granularity optimization of the isValid method.
    * Focuses on concurrent behavior for same/different jdbcUrls with private jdbcUrl field handling.
    */
public class WSConnectionValidationTest {private static volatile int queryExecutionCount = 0;
    private static final String DB_NAME_1 = TestUtils.camelToSnake(WSConsumerAutoCommitTest.class) + "1";
    private static final String DB_NAME_2 = TestUtils.camelToSnake(WSConsumerAutoCommitTest.class) + "2";

    @Mock
    private WSConnection connection1;
    @Mock
    private WSConnection connection2;
    @Mock
    private Statement mockStatement;
    @Mock
    private ResultSet mockResultSet;

    @Mock
    private Transport mockTransport;
    @Mock
    private ConnectionParam mockParam;

    private final String jdbcUrl1 = "jdbc:TAOS://host1:6041/db1";
    private final String jdbcUrl2 = "jdbc:TAOS://host2:6041/db2";
    private static final String URL_FIELD_NAME = "jdbcUrl";
    private static final String TRANSPORT_FIELD_NAME = "transport"; // Transport field name
    private static final String PARAM_FIELD_NAME = "param"; // ConnectionParam field name
    private static Connection gConnection;

    @Before
    public void setup() throws Exception {
        // Skip this test on JDK 16+ due to strong encapsulation of Field.modifiers
        String javaVersion = System.getProperty("java.version");
        int majorVersion;
        if (javaVersion.startsWith("1.")) {
            // JDK 8 format: 1.8.x
            majorVersion = Integer.parseInt(javaVersion.substring(2, 3));
        } else {
            // JDK 9+ format: 11.x, 17.x, etc.
            int dotIndex = javaVersion.indexOf('.');
            majorVersion = Integer.parseInt(dotIndex > 0 ? javaVersion.substring(0, dotIndex) : javaVersion);
        }
        org.junit.Assume.assumeTrue("Test skipped on JDK " + majorVersion + " due to Field.modifiers encapsulation", majorVersion < 16);

        MockitoAnnotations.openMocks(this);

        clearStaticField(WSConnection.class, "conCheckInfoMap");
        clearStaticField(WSConnection.class, "jdbcUrlLocks");
        queryExecutionCount = 0;

        // Key: Let isValid() call the real method, other methods still use mock
        when(connection1.isValid(anyInt())).thenCallRealMethod();
        when(connection2.isValid(anyInt())).thenCallRealMethod();

        setFinalPrivateField(connection1, WSConnection.class, URL_FIELD_NAME, jdbcUrl1);
        setFinalPrivateField(connection2, WSConnection.class, URL_FIELD_NAME, jdbcUrl2);

        setFinalPrivateField(connection1, WSConnection.class, TRANSPORT_FIELD_NAME, mockTransport);
        setFinalPrivateField(connection2, WSConnection.class, TRANSPORT_FIELD_NAME, mockTransport);
        when(mockTransport.isConnectionLost()).thenReturn(false); // Key: Simulate connection not lost

        setFinalPrivateField(connection1, WSConnection.class, PARAM_FIELD_NAME, mockParam);
        when(mockParam.getWsKeepAlive()).thenReturn(300); // 5 minutes = 300,000 milliseconds

        when(connection1.isClosed()).thenReturn(false);
        when(connection2.isClosed()).thenReturn(false);

        when(connection1.createStatement()).thenReturn(mockStatement);
        when(connection2.createStatement()).thenReturn(mockStatement);

        doNothing().when(mockStatement).setQueryTimeout(anyInt());
        doNothing().when(mockStatement).setQueryTimeout(anyInt());
        doNothing().when(mockStatement).close();

        when(mockStatement.executeQuery("SHOW CLUSTER ALIVE")).thenAnswer(invocation -> {
            queryExecutionCount++;
            return mockResultSet;
        });

        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(1);
        doNothing().when(mockResultSet).close();
    }

    @After
    public void teardown() throws Exception {
        clearStaticField(WSConnection.class, "conCheckInfoMap");
        clearStaticField(WSConnection.class, "jdbcUrlLocks");
    }
    @Test
    public void testSameJdbcUrlConcurrency() throws Exception {
        final int THREAD_COUNT = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    connection1.isValid(10);
                } catch (Exception e) {
                    fail("Thread error: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Threads did not complete within timeout", endLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Same JDBC URL should execute query once", 1, queryExecutionCount);
        // Adjust to independent ConCheckInfo class
        Map<String, ConCheckInfo> conCheckInfoMap =
                getPrivateStaticField(WSConnection.class, "conCheckInfoMap");
        assertNotNull("Cache should have entry for jdbcUrl1", conCheckInfoMap.get(jdbcUrl1));
    }

    @Test
    public void testDifferentJdbcUrlConcurrency() throws Exception {
        final int THREADS_PER_URL = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(THREADS_PER_URL * 2);
        ExecutorService executor = Executors.newFixedThreadPool(6);

        for (int i = 0; i < THREADS_PER_URL; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    connection1.isValid(10);
                } catch (Exception e) {
                    fail("jdbcUrl1 thread error: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        for (int i = 0; i < THREADS_PER_URL; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    connection2.isValid(10);
                } catch (Exception e) {
                    fail("jdbcUrl2 thread error: " + e.getMessage());
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue("Threads did not complete within timeout", endLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals("Different JDBC URLs should execute query twice", 2, queryExecutionCount);
        // Adjust to independent ConCheckInfo class
        Map<String, ConCheckInfo> conCheckInfoMap =
                getPrivateStaticField(WSConnection.class, "conCheckInfoMap");
        assertNotNull("Cache should have entry for jdbcUrl1", conCheckInfoMap.get(jdbcUrl1));
        assertNotNull("Cache should have entry for jdbcUrl2", conCheckInfoMap.get(jdbcUrl2));
    }

    @Test
    public void testCacheExpirationTriggersRequery() throws Exception {
        connection1.isValid(10);
        assertEquals(1, queryExecutionCount);

        // Adjust to independent ConCheckInfo class
        Map<String, ConCheckInfo> conCheckInfoMap =
                getPrivateStaticField(WSConnection.class, "conCheckInfoMap");
        ConCheckInfo info = conCheckInfoMap.get(jdbcUrl1);
        assertNotNull("Cache should exist after first call", info);

        Field checkTimeField = findFieldInClassHierarchy(info.getClass(), "lastCheckTime");
        checkTimeField.setAccessible(true);
        checkTimeField.set(info, System.currentTimeMillis() - 1000 * 1000);

        connection1.isValid(10);
        assertEquals(2, queryExecutionCount);
    }

    @Test
    public void testIsValidThrowsExceptionOnConnectionLost() throws Exception {
        TaosAdapterMock mockB = new TaosAdapterMock();
        mockB.start();

        Properties properties = new Properties();
        String url =  "jdbc:TAOS-WS://localhost:" + mockB.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            mockB.stop();

            for (int i = 0; i < 2; i++) {
                Assert.assertFalse(connection.isValid(10));
            }
        }
    }
    @Test
    public void testReConnect() throws Exception {
        TaosAdapterMock mockB = new TaosAdapterMock();
        mockB.start();

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        String url =  "jdbc:TAOS-WS://localhost:" + mockB.getListenPort() + ",localhost:6041/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            assertTrue(connection.isValid(10));
            mockB.stop();
            Thread.sleep(100);
            assertTrue(connection.isValid(10));
        }
    }
    @Test
    public void testCacheSize() throws Exception {
        String url1 =  "jdbc:TAOS-WS://localhost:6041/" + DB_NAME_1 + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        try (Connection connection = DriverManager.getConnection(url1)){
            connection.isValid(10);
        }
        String url2 =  "jdbc:TAOS-WS://localhost:6041/" + DB_NAME_2 + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        try (Connection connection = DriverManager.getConnection(url2)){
            connection.isValid(10);
        }
        Map<String, ConCheckInfo> conCheckInfoMap =
                getPrivateStaticField(WSConnection.class, "conCheckInfoMap");
        Assert.assertEquals(1, conCheckInfoMap.size());
    }
    @Test
    public void testCacheSize2() throws Exception {
        String url1 =  "jdbc:TAOS-WS://localhost:6041/" + DB_NAME_1 + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        try (Connection connection = DriverManager.getConnection(url1)){
            connection.isValid(10);
        }
        TaosAdapterMock mockB = new TaosAdapterMock();
        mockB.start();
        String url2 =  "jdbc:TAOS-WS://localhost:" + mockB.getListenPort() + "/" + DB_NAME_2 +"?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        try (Connection connection = DriverManager.getConnection(url2)){
            connection.isValid(10);
        }
        mockB.stop();
        Map<String, ConCheckInfo> conCheckInfoMap =
                getPrivateStaticField(WSConnection.class, "conCheckInfoMap");

        Assert.assertEquals(2, conCheckInfoMap.size());
    }

    @Test
    public void testConnectionLoss() throws Exception {
        TaosAdapterMock mockB = new TaosAdapterMock();
        mockB.start();
        String url2 =  "jdbc:TAOS-WS://localhost:" + mockB.getListenPort() + "/" + DB_NAME_2 +"?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        try (Connection connection = DriverManager.getConnection(url2)){
            connection.isValid(10);
            mockB.stop();
            Thread.sleep(100);
            Assert.assertFalse(connection.isValid(10));
        }
    }

    private Object getPrivateField(Object target, Class<?> originalClass, String fieldName) throws Exception {
        Field field = findFieldInClassHierarchy(originalClass, fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
    private void setFinalPrivateField(Object target, Class<?> originalClass, String fieldName, Object value) throws Exception {
        Field field = findFieldInClassHierarchy(originalClass, fieldName);
        if (field == null) {
            fail("Field '" + fieldName + "' does not exist in " + originalClass.getName() + " or its superclasses");
        }

        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(target, value);
    }

    private Field findFieldInClassHierarchy(Class<?> clazz, String fieldName) {
        while (clazz != null && clazz != Object.class) {
            try {
                return clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        return null;
    }

    private void clearStaticField(Class<?> clazz, String fieldName) throws Exception {
        Field field = findFieldInClassHierarchy(clazz, fieldName);
        if (field == null) {
            fail("Static field '" + fieldName + "' does not exist in " + clazz.getName() + " or its superclasses");
        }
        field.setAccessible(true);
        Object value = field.get(null);
        if (value instanceof Map) {
            ((Map<?, ?>) value).clear();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getPrivateStaticField(Class<?> clazz, String fieldName) throws Exception {
        Field field = findFieldInClassHierarchy(clazz, fieldName);
        if (field == null) {
            fail("Static field '" + fieldName + "' does not exist in " + clazz.getName() + " or its superclasses");
        }
        field.setAccessible(true);
        return (T) field.get(null);
    }
    /**
        * Invoke private method via reflection
        */
    private Object invokePrivateMethod(Object target, Class<?> originalClass, String methodName, Class<?>[] paramTypes, Object... params) throws Exception {
        Method method = findMethodInClassHierarchy(originalClass, methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(target, params);
    }

    private Method findMethodInClassHierarchy(Class<?> clazz, String methodName, Class<?>[] paramTypes) throws NoSuchMethodException {
        while (clazz != null && clazz != Object.class) {
            try {
                return clazz.getDeclaredMethod(methodName, paramTypes);
            } catch (NoSuchMethodException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchMethodException("Method " + methodName + " does not exist");
    }

    @BeforeClass
    public static void before() throws SQLException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        gConnection = DriverManager.getConnection(url, properties);
        try (Statement statement = gConnection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB_NAME_1);
            statement.executeUpdate("create database if not exists " + DB_NAME_1);
            statement.executeUpdate("drop database if exists " + DB_NAME_2);
            statement.executeUpdate("create database if not exists " + DB_NAME_2);
        }
    }

    @AfterClass
    public static void after() throws InterruptedException, SQLException {
        try (Statement statement = gConnection.createStatement()) {
            statement.executeUpdate("drop database if exists " + DB_NAME_1);
            statement.executeUpdate("drop database if exists " + DB_NAME_2);
        }
        gConnection.close();
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }

}


