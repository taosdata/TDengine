package com.taosdata.tsync.tqueue;

import com.taosdata.tsync.entity.RecordMetadata;
import com.taosdata.tsync.entity.Topic;
import com.taosdata.tsync.entity.TopicPartition;
import com.taosdata.tsync.entity.ProducerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.serializer.Serializer;
import com.taosdata.tsync.serializer.TQueueStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TQueueProducer<T> extends TQueueBase {
    private static final Logger logger = LoggerFactory.getLogger(TQueueProducer.class);

    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private Map<Integer, AtomicLong> partitionOffsets = new HashMap<>();
    private Serializer<T> serializer;

    public TQueueProducer(Properties properties) {
        // establish connection to TQueue
        super(properties);
        this.serializer = new TQueueStringSerializer();

        // init topic-partition offset
        for (String topic : topics.keySet()) {
            Topic t = topics.get(topic);
            for (int pIndex = 1; pIndex <= t.partitions(); pIndex++) {
                int hashCode = TopicPartition.hashCode(topic, pIndex);
                partitionOffsets.put(hashCode, new AtomicLong(0));
            }
        }
    }

    /**
     * 发送一个ProducerRecord，同步的方式,
     *
     * @param record
     * @return
     * @throws Exception
     */
    public RecordMetadata send(ProducerRecord<T> record) throws TQueueException, ExecutionException, InterruptedException {
        Future<RecordMetadata> future = send(record, null);
        return future.get();
    }

    /***
     * 发送一个ProducerRecord，异步的方式，并注册回调函数
     * @param record
     * @param callback
     */
    public Future<RecordMetadata> send(ProducerRecord<T> record, Callback callback) throws TQueueException {
        // check topic
        String topic = record.getTopic();
        if (!topics.containsKey(topic)) {
            flushTopics();
            if (!topics.containsKey(topic)) {
                String message = "topic[ " + topic + " ] not exists";
                logger.error(message);
                throw new TQueueException(message);
            }
        }
        // checkout partition
        int partition = record.getPartition();
        int hashCode = TopicPartition.hashCode(topic, partition);
        if (!partitionOffsets.containsKey(hashCode)) {
            String message = "topic-partition: " + topic + "-" + partition + " not exist";
            logger.error(message);
            throw new TQueueException(message);
        }

        long offset = partitionOffsets.get(hashCode).getAndIncrement();
        Future<RecordMetadata> task = threadPool.submit(new ProducerTask(offset, record));
        if (callback == null) {
            return task;
        }

        try {
            callback.onCompletion(task.get(), null);
        } catch (InterruptedException | ExecutionException e) {
            callback.onCompletion(null, e);
        }
        return task;

    }

    public void close() {
        this.threadPool.shutdownNow();
    }

    private class ProducerTask implements Callable {
        private final long offset;
        private final ProducerRecord record;

        private ProducerTask(long offset, ProducerRecord record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public RecordMetadata call() {
            final String topic = record.getTopic();
            final int partition = record.getPartition();
            long offset = this.offset;
            final long ts = System.currentTimeMillis();

            Object message = record.getMessage();

            final byte[] serializedValue;
            try {
                serializedValue = serializer.serialize((T) message);
            } catch (Exception e) {
                throw new RuntimeException("Can't convert value of class " + message.getClass().getName() + " to byte[] specified in serializer", e);
            }
            final long serializedValueSize = serializedValue.length;
            writeMessage(topic, partition, offset, ts, serializedValue);
            //TODO: cannot get the real offset in TQueue with flushOffset method
            return new RecordMetadata(record.getTopic(), record.getPartition(), offset, ts, serializedValueSize);
        }
    }


}
