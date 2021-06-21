package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.NetToTQueueJobServiceImpl;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NetToTQueueApp extends AbstractApp {
    private static final Logger logger = LoggerFactory.getLogger(NetToTQueueApp.class);
    private static final String helpLine = "Usage: java -jar net-to-tqueue.jar --config <config file path>";

    public static void main(String[] args) throws IOException {

        long start = System.nanoTime();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        System.out.println("start: " + start + ",end: " + end + ", cost: " + (end - start) + " ns");

//        File configFile = readCommandLine(args, helpLine);
//
//        logger.info("NetToTQueueApp started.");
//        // given
//        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
//        JSONObject configJSON = JSONObject.parseObject(IOUtils.toString(new FileInputStream(configFile)));
//        // when
//        Job job = JobFactory.build(ConfigurationType.NET_TO_TQUEUE, configJSON, configurationRepository);
//        // when
//        JobService jobService = new NetToTQueueJobServiceImpl();
//        job.prepare(jobService);
//        // when
//        job.execute(jobService);
//        logger.info("NetToTQueueApp stopped.");
    }


}
