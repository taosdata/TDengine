package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.AffectRowsProcessService;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.ProduceToTQueueJobServiceImpl;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ProduceToTQueueApp extends AbstractApp{

    private final static Logger logger = LoggerFactory.getLogger(ProduceToTQueueApp.class);
    private final static String helpLine = "Usage: java -jar produce-to-tqueue.jar --config <config file path>";

    public static void main(String[] args) throws IOException {
        File configFile = readCommandLine(args, helpLine);

        logger.info("ProduceToTQueueApp started.");
        // init the configuration repository
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        String producerConfigStr = IOUtils.toString(new FileInputStream(configFile));
        JSONObject producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);

        // build job
        Job job = JobFactory.build(ConfigurationType.PRODUCE_TO_TQUEUE, producerTaskConfigJSON, configurationRepository);

        // prepare
        JobService jobService = new ProduceToTQueueJobServiceImpl();
        job.prepare(jobService);

        // execute
        job.execute(jobService);
        logger.info("ProduceToTQueueApp stopped.");

        job.shutdown(jobService);
    }

}