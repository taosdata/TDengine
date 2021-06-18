package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.factory.ProduceJobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.AffectRowsProcessService;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.ProduceJobServiceImpl;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ProduceToTQueueApp {
    private final static Logger logger = LoggerFactory.getLogger(ProduceToTQueueApp.class);

    public static void main(String[] args) throws IOException {
        String configFilepath = null;
        for (int i = 0; i < args.length; i++) {
            if ("--config".equalsIgnoreCase(args[i]) && i < args.length - 1)
                configFilepath = args[++i];
        }
        if (configFilepath == null) {
            printHelp();
            System.exit(0);
        }

        // read config file
        File file = new File(configFilepath);
        if (!file.exists()) {
            logger.error("cannot find config file: " + configFilepath);
            System.exit(-1);
        }

        // init the configuration repository
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        String producerConfigStr = IOUtils.toString(new FileInputStream(file));
        JSONObject producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);

        // build job
        Job job = ProduceJobFactory.build(producerTaskConfigJSON, configurationRepository);

        // prepare
        JobService jobService = new ProduceJobServiceImpl(new AffectRowsProcessService());
        job.prepare(jobService);

        // execute
        job.execute(jobService);
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JDBCDemo.jar --config <config file path>");
    }
}