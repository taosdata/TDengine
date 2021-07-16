package com.taosdata.tsync.tqueue;

import com.taosdata.tsync.entity.RecordMetadata;

public interface Callback {
    void onCompletion(RecordMetadata metadata, Exception exception);
}
