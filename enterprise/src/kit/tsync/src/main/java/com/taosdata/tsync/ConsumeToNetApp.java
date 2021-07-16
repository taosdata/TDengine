package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeToNetJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.NetToTQueueJobServiceImpl;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ConsumeToNetApp extends AbstractApp {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToNetApp.class);
    private static final String helpLine = "Usage: java -jar consume-to-net.jar --config <config file path>";

    public static void main(String[] args) throws IOException {
        File configFile = readCommandLine(args, helpLine);

        logger.info("ConsumeToNetApp started.");
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        JSONObject configJSON = JSONObject.parseObject(IOUtils.toString(new FileInputStream(configFile)));
        // when
        Job job = JobFactory.build(ConfigurationType.CONSUME_TO_NET, configJSON, configurationRepository);
        // when
        JobService jobService = new ConsumeToNetJobServiceImpl();
        job.prepare(jobService);
        // when
        job.execute(jobService);
        logger.info("ConsumeToNetApp stopped.");
    }


}
