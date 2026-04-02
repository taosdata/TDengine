package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaosInfoTest {

    private TaosInfo taosInfo;

    @Before
    public void setUp() {
        taosInfo = TaosInfo.getInstance();
    }

    @Test
    public void testGetInstance() {
        TaosInfo instance1 = TaosInfo.getInstance();
        TaosInfo instance2 = TaosInfo.getInstance();
        Assert.assertSame(instance1, instance2);
    }

    @Test
    public void testGetConnectOpen() {
        long initial = taosInfo.getConnectOpen();
        taosInfo.connOpenIncrement();
        Assert.assertEquals(initial + 1, taosInfo.getConnectOpen());
    }

    @Test
    public void testGetConnectClose() {
        long initial = taosInfo.getConnectClose();
        taosInfo.connectCloseIncrement();
        Assert.assertEquals(initial + 1, taosInfo.getConnectClose());
    }

    @Test
    public void testGetStatementCount() {
        long initial = taosInfo.getStatementCount();
        taosInfo.stmtCountIncrement();
        Assert.assertEquals(initial + 1, taosInfo.getStatementCount());
    }

    @Test
    public void testGetConnectActive() {
        long initial = taosInfo.getConnect_active();
        taosInfo.connOpenIncrement();
        Assert.assertEquals(initial + 1, taosInfo.getConnect_active());

        taosInfo.connectCloseIncrement();
        Assert.assertEquals(initial, taosInfo.getConnect_active());
    }

    @Test
    public void testMultipleIncrements() {
        long initialOpen = taosInfo.getConnectOpen();
        long initialClose = taosInfo.getConnectClose();
        long initialStmt = taosInfo.getStatementCount();

        taosInfo.connOpenIncrement();
        taosInfo.connOpenIncrement();
        taosInfo.connOpenIncrement();

        Assert.assertEquals(initialOpen + 3, taosInfo.getConnectOpen());

        taosInfo.connectCloseIncrement();
        taosInfo.connectCloseIncrement();

        Assert.assertEquals(initialClose + 2, taosInfo.getConnectClose());

        taosInfo.stmtCountIncrement();
        taosInfo.stmtCountIncrement();
        taosInfo.stmtCountIncrement();
        taosInfo.stmtCountIncrement();

        Assert.assertEquals(initialStmt + 4, taosInfo.getStatementCount());
    }

    @Test
    public void testConcurrentIncrements() throws InterruptedException {
        long initialOpen = taosInfo.getConnectOpen();
        long initialStmt = taosInfo.getStatementCount();

        int threadCount = 10;
        int incrementsPerThread = 100;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    taosInfo.connOpenIncrement();
                    taosInfo.stmtCountIncrement();
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(initialOpen + threadCount * incrementsPerThread, taosInfo.getConnectOpen());
        Assert.assertEquals(initialStmt + threadCount * incrementsPerThread, taosInfo.getStatementCount());
    }

    @Test
    public void testInitialState() {
        TaosInfo info = TaosInfo.getInstance();
        // Should not throw any exception
        Assert.assertNotNull(info);
        Assert.assertTrue(info.getConnectOpen() >= 0);
        Assert.assertTrue(info.getConnectClose() >= 0);
        Assert.assertTrue(info.getConnect_active() >= 0);
        Assert.assertTrue(info.getStatementCount() >= 0);
    }

    @Test
    public void testConnectActiveWithMultipleOpensAndCloses() {
        long initial = taosInfo.getConnect_active();

        taosInfo.connOpenIncrement();
        taosInfo.connOpenIncrement();
        taosInfo.connOpenIncrement();
        Assert.assertEquals(initial + 3, taosInfo.getConnect_active());

        taosInfo.connectCloseIncrement();
        Assert.assertEquals(initial + 2, taosInfo.getConnect_active());

        taosInfo.connectCloseIncrement();
        Assert.assertEquals(initial + 1, taosInfo.getConnect_active());

        taosInfo.connectCloseIncrement();
        Assert.assertEquals(initial, taosInfo.getConnect_active());
    }
}
