package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.Configuration;

public interface ProduceJobConfigPrepareService {
    void prepare(Configuration jobConfiguration);
}
