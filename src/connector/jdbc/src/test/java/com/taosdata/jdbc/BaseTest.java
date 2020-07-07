package com.taosdata.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import com.taosdata.jdbc.utils.TDNodes;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseTest {

    private static boolean testCluster = false;
    private static String deployPath = System.getProperty("user.dir");     
    private static int valgrind = 0;    
    
    @BeforeClass
    public static void setupEnv() {
        try{
            File file = new File(deployPath + "/../../../");
            String rootPath = file.getCanonicalPath();

            TDNodes tdNodes = new TDNodes();            
            tdNodes.setPath(rootPath);
            tdNodes.setTestCluster(testCluster);
            tdNodes.setValgrid(valgrind);

            tdNodes.deploy(1);
            tdNodes.start(1);  

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void clearUpEnv() {
        
    }
}