package com.taosdata.tsync;

import com.taosdata.tsync.domain.ProducerRecord;
import com.taosdata.tsync.domain.RecordMetadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TQueueProducer extends TQueueBase {

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    public TQueueProducer(Properties properties) {
        super(properties);
    }

    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        String topic = record.getTopic();
        if (!topics.containsKey(topic)) {
            throw new Exception("topic[ " + topic + " ] not exists");
        }

        Future<RecordMetadata> task = threadPool.submit(new ProducerTask(record));
        return task;
    }

    public void send(ProducerRecord record, Callback callback) {
        String topic = record.getTopic();
        if (!topics.containsKey(topic)) {
            callback.onCompletion(null, new Exception("topic[ " + topic + " ] not exists"));
            return;
        }

        Future<RecordMetadata> task = threadPool.submit(new ProducerTask(record));
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = task.get();
            callback.onCompletion(recordMetadata, null);
        } catch (InterruptedException | ExecutionException e) {
            callback.onCompletion(null, e);
        }

    }

    private class ProducerTask implements Callable {
        private final ProducerRecord record;
        private volatile AtomicLong offset = new AtomicLong();

        private ProducerTask(ProducerRecord record) {
            this.record = record;
        }

        @Override
        public RecordMetadata call() throws Exception {
            final String topic = record.getTopic();
            final int partition = record.getPartition();
            long offset = this.offset.getAndIncrement();
            final long ts = System.currentTimeMillis();
            final byte[] messages = record.getMessage().getBytes();
            final long serializedValueSize = messages.length;

            final String update = "insert into " + topic + ".p" + partition + "(off, ts, content) values(?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(update)) {
                pstmt.setLong(1, offset);
                pstmt.setTimestamp(2, new Timestamp(ts));
                pstmt.setBytes(3, messages);
                pstmt.executeUpdate();
            }

            try (Statement stmt = connection.createStatement()) {
                final String query = "select last_row(off) from " + topic + ".p" + partition;
                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                offset = rs.getTimestamp(1).getTime();
            }

            return new RecordMetadata(record.getTopic(), record.getPartition(), offset, ts, serializedValueSize);
        }
    }


}
