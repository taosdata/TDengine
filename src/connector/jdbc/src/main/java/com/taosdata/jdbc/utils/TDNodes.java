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

    public void setPath(String path) {
        try {
            String psCmd = "ps -ef|grep -w taosd| grep -v grep | awk '{print $2}'" ;
            Process ps = Runtime.getRuntime().exec(psCmd);
            ps.waitFor();
            String killCmd = "kill -9 " + ps.pid();
            Runtime.getRuntime().exec(killCmd).waitFor();        

            String binPath = System.getProperty("user.dir");
            binPath += "/../../../debug";
            System.out.println("binPath: " + binPath);
        
            File file = new File(path);
            binPath = file.getCanonicalPath();
            System.out.println("binPath real path: " + binPath);
                
            if(path.isEmpty()){
                file = new File(path + "/../../");                
                path = file.getCanonicalPath();
            }            

            for(int i = 0; i < tdNodes.size(); i++) {
                tdNodes.get(i).setPath(path);
            }
        } catch (Exception e) {
            e.printStackTrace();
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