package com.taosdata.jdbc.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerManagerTest {

    private StringWriter logOutput;

    @Before
    public void setUp() {
        logOutput = new StringWriter();
        ConsumerManager.setLogWriter(new PrintWriter(logOutput));
    }

    @After
    public void tearDown() {
        ConsumerManager.setLogWriter(null);
    }

    @Test
    public void testRegister() {
        ConsumerFactory mockFactory = mock(ConsumerFactory.class);
        ConsumerManager.register(mockFactory);

        assertTrue(logOutput.toString().contains("registerConsumerFactory"));
    }

    @Test(expected = NullPointerException.class)
    public void testRegister_NullFactory() {
        ConsumerManager.register(null);
    }
    @Test(expected = SQLException.class)
    public void testGetConsumer_NoSuitableConsumer() throws SQLException {
        ConsumerFactory mockFactory = mock(ConsumerFactory.class);
        when(mockFactory.acceptsType("testType")).thenReturn(false);

        ConsumerManager.register(mockFactory);
        ConsumerManager.getConsumer("testType");
    }

    @Test
    public void testGetLogWriter() {
        PrintWriter writer = ConsumerManager.getLogWriter();
        assertNotNull(writer);
    }

    @Test
    public void testSetLogWriter() {
        PrintWriter newWriter = new PrintWriter(new StringWriter());
        ConsumerManager.setLogWriter(newWriter);

        assertEquals(newWriter, ConsumerManager.getLogWriter());
    }

    @Test
    public void testPrintln() {
        ConsumerManager.println("Test message");
        assertTrue(logOutput.toString().contains("Test message"));
    }
}