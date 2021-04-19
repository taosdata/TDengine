package com.taosdata.tsync.domain;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ConsumerRecords implements Iterable<ConsumerRecord> {

    @Override
    public Iterator<ConsumerRecord> iterator() {
        return null;
    }

    @Override
    public void forEach(Consumer<? super ConsumerRecord> action) {

    }

    @Override
    public Spliterator<ConsumerRecord> spliterator() {
        return null;
    }
}