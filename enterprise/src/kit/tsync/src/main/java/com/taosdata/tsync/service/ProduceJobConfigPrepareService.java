package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.Configuration;

public interface ProduceJobConfigPrepareService {
    void prepare(Configuration jobConfiguration);
}
