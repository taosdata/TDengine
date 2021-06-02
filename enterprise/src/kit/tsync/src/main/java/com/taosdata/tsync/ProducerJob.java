package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import com.taosdata.tsync.service.ProduceJobConfigPrepareService;
import com.taosdata.tsync.service.ProduceTaskArrangeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProducerJob {
    private static final Logger logger = LoggerFactory.getLogger(ProducerJob.class);

    private JSONObject configJSON;
    private ProduceJobConfigPrepareService prepareService;
    private ProduceTaskArrangeService arrangeService;

    private Configuration jobConfiguration;
    private List<Thread> produceTaskList;

    public ProducerJob(JSONObject configJSON, ProduceJobConfigPrepareService prepareService, ProduceTaskArrangeService arrangeService) {
        this.configJSON = configJSON;
        this.prepareService = prepareService;
        this.arrangeService = arrangeService;
    }

    public void execute() {
        logger.info(">>> producerJob started.");
        // 1. load configuration
        jobConfiguration = ConfigurationFactory.build(ConfigurationType.PRODUCE_JOB, configJSON);
        // 2. prepare
        prepareService.prepare(jobConfiguration);
        // 3. create threads
        produceTaskList = arrangeService.createThreads(jobConfiguration,
                () -> System.out.println(Thread.currentThread().getName() + " is running."));
        // 4. start threads
        produceTaskList.forEach(Thread::start);
        // 5. wait until all threads finished
        try {
            for (Thread t : produceTaskList) {
                t.join();
            }
        } catch (InterruptedException e) {
            logger.error(">>> producerJob interrupted.");
            e.printStackTrace();
        }
        logger.info(">>> producerJob finished.");
    }
}
