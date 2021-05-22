package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.Configuration;

import java.util.List;

public interface ProduceTaskArrangeService {

    List<Thread> createThreads(Configuration jobConfiguration, Runnable produceTask);
}