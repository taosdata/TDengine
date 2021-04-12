package com.taosdata.tsync;

import com.taosdata.tsync.domain.RecordMetadata;

public interface Callback {
    void onCompletion(RecordMetadata metadata, Exception exception);
}
