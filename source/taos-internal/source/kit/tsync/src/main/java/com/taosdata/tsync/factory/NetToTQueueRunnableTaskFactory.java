package com.taosdata.tsync.factory;

import com.taosdata.tsync.service.NetToTQueueRunnableTask;
import com.taosdata.tsync.tqueue.TQueueProducer;

public class NetToTQueueRunnableTaskFactory {

    private final NetToTQueueRunnableTask instance;


    public NetToTQueueRunnableTaskFactory() {
        instance = new NetToTQueueRunnableTask();
    }

    public NetToTQueueRunnableTaskFactory setProducer(TQueueProducer producer) {
        instance.setProducer(producer);
        return this;
    }

    public NetToTQueueRunnableTaskFactory setListeningPort(int port) {
        instance.setListeningPort(port);
        return this;
    }

    public NetToTQueueRunnableTask build() {
        return instance;
    }

}
