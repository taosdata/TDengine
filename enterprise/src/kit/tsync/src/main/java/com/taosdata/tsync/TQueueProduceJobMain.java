package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.factory.ProduceJobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.AffectRowsProcessService;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.ProduceJobServiceImpl;
import com.taosdata.tsync.service.ResultProcessService;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.Objects;

public class TQueueProduceJobMain {

    public static void main(String[] args) throws IOException {
        // init the configuration repository
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

        // read config
        String producerConfigStr = IOUtils.toString(Objects.requireNonNull(TQueueProduceJobMain.class.getClassLoader().getResourceAsStream("producer-job.json")));
        JSONObject producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);


        ResultProcessService resultProcessService = new AffectRowsProcessService();
        JobService jobService = new ProduceJobServiceImpl(resultProcessService);

        // when

        Job job = ProduceJobFactory.build(producerTaskConfigJSON, configurationRepository);

        // when
        Configuration configuration = job.getConfiguration(jobService);

        // when
        job.prepare(jobService);

        // when
        job.execute(jobService);
    }
}