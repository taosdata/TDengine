package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.TDNodes;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseTest {

    private static boolean testCluster = false;
    private static TDNodes nodes = new TDNodes();

    @BeforeClass
    public static void setupEnv() {
        try {
            if (nodes.getTDNode(1).getTaosdPid() != null) {
                System.out.println("Kill taosd before running JDBC test");
                nodes.getTDNode(1).setRunning(1);
                nodes.stop(1);
            }
            nodes.setTestCluster(testCluster);
            nodes.deploy(1);
            nodes.start(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void cleanUpEnv() {
        nodes.stop(1);
    }
}