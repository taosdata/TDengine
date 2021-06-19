package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ConsumeToTDengineApp {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineApp.class);

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

        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        JSONObject consumerTaskConfigJSON = JSONObject.parseObject(IOUtils.toString(new FileInputStream(file)));
        // when
        Job job = JobFactory.build(ConfigurationType.CONSUME_TO_TDENGINE, consumerTaskConfigJSON, configurationRepository);

        // when
        JobService jobService = new ConsumeJobServiceImpl();
        job.prepare(jobService);

        // when
        job.execute(jobService);


    }

    private static void printHelp() {
        System.out.println("Usage: java -jar ConsumeToTDengine.jar --config <config file path>");
    }

}
