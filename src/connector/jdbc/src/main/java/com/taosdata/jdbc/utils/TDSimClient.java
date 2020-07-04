package com.taosdata.jdbc.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TDSimClient {
    
    private boolean testCluster;
    private String path;
    private String cfgDir;
    private String logDir;
    private String cfgPath;
    
    public TDSimClient() {
        testCluster = false;        
    }

    public void setTestCluster(boolean testCluster) {
        this.testCluster = testCluster;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setCfgConfig(String option, String value) {
        String cmd = "echo " + option + " " +  value + " >> " + this.cfgPath;
        
        try {
            Process ps = Runtime.getRuntime().exec(cmd);            

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while(br.readLine() != null) {
                System.out.println(br.readLine());
            }  
            
            ps.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deploy() {
        this.logDir = this.path + "/sim/psim/log";
        System.out.println("======logDir: " + logDir + "=====");
        this.cfgDir = this.path + "/sim/psim/cfg";
        System.out.println("======cfgDir: " + cfgDir + "=====");
        this.cfgPath = this.path + "/sim/psim/cfg/taos.cfg";
        System.out.println("======cfgPath: " + cfgPath + "=====");

        try {
            String cmd = "rm -rf " + this.logDir;
            Runtime.getRuntime().exec(cmd).waitFor();
            
            cmd = "rm -rf " + this.cfgDir;
            Runtime.getRuntime().exec(cmd).waitFor();

            cmd = "mkdir -p " + this.logDir;
            Runtime.getRuntime().exec(cmd).waitFor();

            cmd = "mkdir -p " + this.cfgDir;
            Runtime.getRuntime().exec(cmd).waitFor();

            cmd = "touch " + this.cfgPath;
            Runtime.getRuntime().exec(cmd).waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(this.testCluster) {
            setCfgConfig("masterIp", "192.168.0.1");
            setCfgConfig("secondIp", "192.168.0.2");
        }
        setCfgConfig("logDir", this.logDir);
        setCfgConfig("numOfLogLines", "100000000");
        setCfgConfig("numOfThreadsPerCore", "2.0");
        setCfgConfig("locale", "en_US.UTF-8");
        setCfgConfig("charset", "UTF-8");
        setCfgConfig("asyncLog", "0");
        setCfgConfig("anyIp", "0");
        setCfgConfig("sdbDebugFlag", "135");
        setCfgConfig("rpcDebugFlag", "135");
        setCfgConfig("tmrDebugFlag", "131");
        setCfgConfig("cDebugFlag", "135");
        setCfgConfig("udebugFlag", "135");
        setCfgConfig("jnidebugFlag", "135");
        setCfgConfig("qdebugFlag", "135");
    }


}