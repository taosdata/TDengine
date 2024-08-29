package com.taosdata;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.tmq.TMQConstants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.taosdata.Config.*;

public class ConsumerDemo {
    public static void main(String[] args) throws Exception {
        // Config
        String configFilePath = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-c")) {
                if (i + 1 < args.length) {
                    configFilePath = args[i + 1];
                    break;
                }
            }
        }
        if (configFilePath == null) {
            configFilePath = "./alert.json";
        }


        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(configFilePath))) {
            String line;
            while ((line = br.readLine())!= null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        String fileContent = content.toString();


        AlertConfig alertConfig = JSON.parseObject(fileContent, AlertConfig.class);

        if (alertConfig.getNumericAlertList() == null){
            alertConfig.setNumericAlertList(new ArrayList<>());
        }

        if (alertConfig.getBoolAlertList() == null){
            alertConfig.setBoolAlertList(new ArrayList<>());
        }


        System.out.println("Config: " + JSON.toJSONString(alertConfig));


        Properties prop = new Properties();
        prop.setProperty(TMQConstants.CONNECT_TYPE, alertConfig.getProtocol());
        prop.setProperty(TMQConstants.BOOTSTRAP_SERVERS, alertConfig.getHost() + ":" + alertConfig.getPort());
        prop.setProperty(TMQConstants.CONNECT_USER, "root");
        prop.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        prop.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        prop.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        prop.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        prop.setProperty(TMQConstants.GROUP_ID, "gId");

        try {
            Worker worker = new Worker(prop, alertConfig);
            worker.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
