package com.taosdata.tsync;

public interface Callback {
    void onCompletion(RecordMetadata metadata, Exception exception);
}
