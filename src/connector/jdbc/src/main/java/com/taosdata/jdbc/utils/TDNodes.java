package com.taosdata.jdbc.utils;

import java.io.File;
import java.util.*;

public class TDNodes {
    private ArrayList<TDNode> tdNodes; 
    private boolean testCluster;     

    public TDNodes () {
        tdNodes = new ArrayList<>();
        for(int i = 1; i < 11; i ++) {            
            tdNodes.add(new TDNode(i));
        }               
    }

    public void setTestCluster(boolean testCluster) {
        this.testCluster = testCluster;
    }

    public void check(int index) {
        if(index < 1 || index > 10) {
            System.out.println("index: " + index + " should on a scale of [1, 10]");
            return;
        }
    } 

    public void deploy(int index) {
        try {     
            File file = new File(System.getProperty("user.dir") + "/../../../");
            String projectRealPath = file.getCanonicalPath();
            check(index);        
            tdNodes.get(index - 1).setTestCluster(this.testCluster);            
            tdNodes.get(index - 1).setPath(projectRealPath); 
            tdNodes.get(index - 1).deploy();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("deploy Test Exception");
        }
    }
    
    public void cfg(int index, String option, String value) {
        check(index);
        tdNodes.get(index - 1).setCfgConfig(option, value);
    }

    public TDNode getTDNode(int index) {
        check(index);
        return tdNodes.get(index - 1);
    }
    
    public void start(int index) {
        check(index);
        tdNodes.get(index - 1).start();
    }

    public void stop(int index) {
        check(index);
        tdNodes.get(index - 1).stop();
    }
    
    public void startIP(int index) {
        check(index);
        tdNodes.get(index - 1).startIP();
    }

    public void stopIP(int index) {
        check(index);
        tdNodes.get(index - 1).stopIP();
    }

}