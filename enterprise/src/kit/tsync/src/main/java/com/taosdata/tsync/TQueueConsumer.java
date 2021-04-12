package com.taosdata.tsync;

import com.taosdata.tsync.domain.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TQueueConsumer extends TQueueBase {

    public TQueueConsumer(Properties properties) {
        super(properties);
    }

    public void subscribe(List<String> test) {

    }

    public ConsumerRecords poll(Duration ofMillis) {
        return null;
    }
}