package com.taosdata.jdbc;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.BeforeClass;

public class BaseTest {
    
    @BeforeClass
    public static void setupEnv() {
        try{
            String path = System.getProperty("user.dir");
            String bashPath = path + "/buildTDengine.sh";

            Process ps = Runtime.getRuntime().exec(bashPath);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while(br.readLine() != null) {
                System.out.println(br.readLine());
            }            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}