package com.taosdata.jdbc.tmq;

import java.util.Map;

@FunctionalInterface
public interface OffsetCommitCallback<V> {

    void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception);
}
