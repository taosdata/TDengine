package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConsumeToTDengineRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.factory.TaosdConnectionFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConsumeToTDengineJobServiceImpl extends AbstractRunnableJobService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineJobServiceImpl.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

    private Multimap<Integer, Integer> threadIndex2PartitionList;

    public ConsumeToTDengineJobServiceImpl() {
        super();
    }

    private ConsumerConfiguration consumerConfiguration;
    private TQueueConsumer consumer;
    private TaosdConfiguration taosdConfiguration;
    private TaskConfiguration taskConfiguration;
    private int pollingInterval;
    private SchemaMissingStrategy schemaMissing;
    private SchemaConfiguration schemaConfiguration;

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {
        ConsumeToTDengineConfiguration configuration = (ConsumeToTDengineConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // consumer
        consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        consumer = TQueueConsumerFactory.build(consumerConfiguration);

        // topic, partitions
        taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        String topic = taskConfiguration.getTopic();
        if (topic == null || !consumer.containsTopic(topic)) {
            String errMsg = "topic[" + topic + "] does not exist";
            logger.error(errMsg);
            throw new TsyncException(errMsg);
        }
        int[] partitions = taskConfiguration.getPartitions();
        // TODO: check partitions

        // arrange threads to partitions
        int threadSize = taskConfiguration.getThreads();
        threadIndex2PartitionList = Utils.divideArrIntoGroups(partitions, threadSize);
        int actualThreads = threadIndex2PartitionList.keySet().size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;

        // destination - taosd
        taosdConfiguration = (TaosdConfiguration) configuration.findFirst(ConfigurationType.TAOSD);
        if (taosdConfiguration == null) {
            String errorMsg = "cannot find taosd in configurations";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // destination - strategy
        StrategyConfiguration strategyConfiguration = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        pollingInterval = strategyConfiguration.getPollingInterval();
        schemaMissing = strategyConfiguration.getSchemaMissing();

        if (schemaMissing == SchemaMissingStrategy.CREATE) {
            schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);
            doCreateSchema();
        }

        // create runnable tasks
        List<UUID> taskIds = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);
            List<Integer> partitionsToWrite = new ArrayList<>(threadIndex2PartitionList.get(i));
            Connection connection = TaosdConnectionFactory.build(taosdConfiguration);

            // callable task
            ConsumeToTDengineRunnableTask runnable = new ConsumeToTDengineRunnableTaskFactory()
                    .setConsumer(consumer)
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTopic(topic)
                    .setTaosdConnection(connection)
                    .setPollingInterval(pollingInterval)
                    .build();

            RunnableTask runnableTask = new RunnableTask(runnable);
            runnableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }
        return taskIds;
    }

    private void doCreateSchema() {
        DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) schemaConfiguration.findFirst(ConfigurationType.DATABASE);
        String dbname = databaseConfiguration.getName();
        Connection taosdConnection = TaosdConnectionFactory.build(taosdConfiguration);
        if (isDatabaseMissing(taosdConnection, dbname)) {
            doCreateDatabase(taosdConnection, dbname);
        }

        StableConfiguration stableConfiguration = (StableConfiguration) schemaConfiguration.findFirst(ConfigurationType.STABLE);
        String stableName = stableConfiguration.getName();
        if (isStableMissing(taosdConnection, dbname, stableName)) {
            doCreateSuperTable(taosdConnection, stableConfiguration);
        }
    }

    private void doCreateDatabase(Connection taosdConnection, String dbname) {
        try (Statement stmt = taosdConnection.createStatement()) {
            stmt.execute("create database if not exists " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isDatabaseMissing(Connection taosdConnection, String dbname) {
        return !showDatabases(taosdConnection).contains(dbname);
    }

    private boolean isStableMissing(Connection taosdConnection, String dbname, String stableName) {
        return !showStables(taosdConnection, dbname).contains(stableName);
    }

    // create table xxx (xx xx, xx xx, xx xx) tags(xx xx, xx xx)
    private void doCreateSuperTable(Connection taosdConnection, StableConfiguration stableConfiguration) {
        // table name
        String stableName = stableConfiguration.getName();
        // columns
        List<Configuration> columns = stableConfiguration.find(ConfigurationType.COLUMN);
        String columnsStr = columns.stream().map(configuration -> {
            ColumnConfiguration column = (ColumnConfiguration) configuration;
            String name = column.getName();
            String type = column.getType();
            Integer length = column.getLength();
            if ("nchar".equalsIgnoreCase(type) || "binary".equalsIgnoreCase(type)) {
                return name + " " + type + "(" + length + ")";
            }
            return name + " " + type;
        }).collect(Collectors.joining(",", "(", ")"));
        // tags
        List<Configuration> tags = stableConfiguration.find(ConfigurationType.TAG);
        String tagStr = tags.stream().map(configuration -> {
            TagConfiguration tag = (TagConfiguration) configuration;
            String name = tag.getName();
            String type = tag.getType();
            Integer length = tag.getLength();
            if ("nchar".equalsIgnoreCase(type) || "binary".equalsIgnoreCase(type)) {
                return name + " " + type + "(" + length + ")";
            }
            return name + " " + type;
        }).collect(Collectors.joining(",", "(", ")"));

        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(stableName).append(" ").append(columnsStr).append(" tags").append(tagStr);

        try (Statement stmt = taosdConnection.createStatement()) {
            stmt.execute(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<String> showDatabases(Connection taosdConnection) {
        List<String> databases = new ArrayList<>();
        try (Statement stmt = taosdConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                String dbname = rs.getString("name");
                databases.add(dbname);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return databases;
    }

    private List<String> showStables(Connection taosdConnection, String database) {
        List<String> stables = new ArrayList<>();
        try (Statement stmt = taosdConnection.createStatement()) {
            stmt.execute("use " + database);
            ResultSet rs = stmt.executeQuery("show stables");
            while (rs.next()) {
                String dbname = rs.getString("name");
                stables.add(dbname);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stables;
    }


}
