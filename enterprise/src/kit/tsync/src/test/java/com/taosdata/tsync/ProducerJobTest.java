package com.taosdata.tsync;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.service.ProduceJobConfigPrepareService;
import com.taosdata.tsync.service.ProduceJobConfigPrepareServiceImpl;
import com.taosdata.tsync.service.ProduceTaskArrangeService;
import com.taosdata.tsync.service.ProduceTaskArrangeServiceImpl;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class ProducerJobTest {

    private JSONObject configJSON;
    private ProduceJobConfigPrepareService prepareService;
    private ProduceTaskArrangeService arrangeService;

    private ProducerJob job;

    @Test
    public void execute() {
        // given
        prepareService = new ProduceJobConfigPrepareServiceImpl();
        arrangeService = new ProduceTaskArrangeServiceImpl();
        job = new ProducerJob(configJSON, prepareService, arrangeService);

        // when
        job.execute();

        // then

    }

    @Before
    public void before() throws IOException {
        // read config file
        InputStream is = getClass().getClassLoader().getResourceAsStream("producer-task.json");
        String configStr = IOUtils.toString(is);
        configJSON = JSONObject.parseObject(configStr);
    }
}