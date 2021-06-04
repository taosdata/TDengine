package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.Configuration;

import java.util.List;

public class ProduceTaskArrangeServiceImpl implements ProduceTaskArrangeService{

    @Override
    public List<Thread> createThreads(Configuration jobConfiguration, Runnable produceTask) {
        return null;
    }
}