package com.taosdata.jdbc;

import java.io.File;
import com.taosdata.jdbc.utils.TDNodes;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseTest {

    private static boolean testCluster = false;
    private static String deployPath = System.getProperty("user.dir");     
    private static int valgrind = 0;
    private static TDNodes tdNodes = new TDNodes();    
    
    
    @BeforeClass
    public static void setUpEvn() {
        try{
            File file = new File(deployPath + "/../../../");
            String rootPath = file.getCanonicalPath();
                       
            tdNodes.setPath(rootPath);
            tdNodes.setTestCluster(testCluster);
            tdNodes.setValgrid(valgrind);

            tdNodes.deploy(1);
            tdNodes.start(1);  

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Base Test Exception");
        }
    }

    @AfterClass
    public static void cleanUpEnv() {
        tdNodes.stop(1);
    }
}