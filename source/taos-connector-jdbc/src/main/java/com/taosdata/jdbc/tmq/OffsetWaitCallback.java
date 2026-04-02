package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBError;

import java.util.Map;

import static com.taosdata.jdbc.TSDBConstants.TMQ_SUCCESS;

public class OffsetWaitCallback<V> {
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    private final JNIConsumer<?> consumer;
    private final OffsetCommitCallback<V> callback;

    public OffsetWaitCallback(Map<TopicPartition, OffsetAndMetadata> offsets, JNIConsumer<?> consumer, OffsetCommitCallback<V> callback) {
        this.offsets = offsets;
        this.consumer = consumer;
        this.callback = callback;
    }

    @SuppressWarnings("unused")
    public void commitCallbackHandler(int code) {
        if (TMQ_SUCCESS != code) {
            Exception exception = TSDBError.createSQLException(code, consumer.getErrMsg(code));

            callback.onComplete(offsets, exception);
        } else {
            callback.onComplete(offsets, null);
        }

    }
}
