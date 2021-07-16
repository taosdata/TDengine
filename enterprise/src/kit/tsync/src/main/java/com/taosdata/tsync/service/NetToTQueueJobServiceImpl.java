package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.NetConfiguration;
import com.taosdata.tsync.entity.config.NetToTQueueConfiguration;
import com.taosdata.tsync.entity.config.ProducerConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.NetToTQueueRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueProducerFactory;
import com.taosdata.tsync.tqueue.TQueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NetToTQueueJobServiceImpl extends AbstractRunnableJobService {

    private static final Logger logger = LoggerFactory.getLogger(NetToTQueueJobServiceImpl.class);

    private TQueueProducer<String> producer;

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {

        Configuration config = configurationRepository.find(configurationId);
        if (config == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        NetToTQueueConfiguration configuration = (NetToTQueueConfiguration) config;

        // 1. producer
        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);
        producer = TQueueProducerFactory.build(producerConfiguration);

        // 2. net
        NetConfiguration netConfiguration = (NetConfiguration) configuration.findFirst(ConfigurationType.NET);
        int listeningPort = netConfiguration.getPort();

        // 3. create Runnable Tasks
        NetToTQueueRunnableTask netToTQueueRunnableTask = new NetToTQueueRunnableTaskFactory()
                .setListeningPort(listeningPort)
                .setProducer(producer)
                .build();
        RunnableTask task = new RunnableTask(netToTQueueRunnableTask);
        runnableTaskRepository.add(task);
        List<UUID> taskIds = new ArrayList<>();
        taskIds.add(task.getId());
        return taskIds;
    }

    @Override
    public void shutdown() {
        producer.close();
    }

}
