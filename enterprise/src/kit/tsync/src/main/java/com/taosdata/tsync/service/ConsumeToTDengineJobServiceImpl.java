package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.DatabasePrecision;
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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToTDengineJobServiceImpl extends AbstractRunnableJobService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineJobServiceImpl.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

    private final List<UUID> taskIds = new ArrayList<>();

    private ConsumerConfiguration consumerConfiguration;
    private TQueueConsumer consumer;
    private TaosdConfiguration taosdConfiguration;
    private TaskConfiguration taskConfiguration;
    private int pollingInterval;
    private SchemaMissingStrategy schemaMissing;
    private SchemaConfiguration schemaConfiguration;

    private PrintCountRunnableTask printCountRunnable;

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

        List<Integer[]> threadIndex2PartitionList = Utils.divideArrayIntoGroups(partitions, threadSize);

        int actualThreads = threadIndex2PartitionList.size();
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
        List<ConsumeToTDengineRunnableTask> runnableTasks = IntStream.range(0, threadSize).mapToObj(i -> {
            List<Integer> partitionsToWrite = Arrays.stream(threadIndex2PartitionList.get(i)).collect(Collectors.toList());
            Connection connection = TaosdConnectionFactory.build(taosdConfiguration);

            // callable task
            ConsumeToTDengineRunnableTask runnable = new ConsumeToTDengineRunnableTaskFactory()
                    .setConsumer(TQueueConsumerFactory.build(consumerConfiguration))
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTopic(topic)
                    .setTaosdConnection(connection)
                    .setPollingInterval(pollingInterval)
                    .build();
            return runnable;
        }).collect(Collectors.toList());


        runnableTasks.stream().map(RunnableTask::new).forEach(i -> {
            runnableTaskRepository.add(i);
            taskIds.add(i.getId());
        });
        // create print count thread
        printCountRunnable = new PrintCountRunnableTask(runnableTasks.get(0), 10 * 1000);
        RunnableTask printCountRunnableTask = new RunnableTask(printCountRunnable);
        runnableTaskRepository.add(printCountRunnableTask);
        taskIds.add(printCountRunnableTask.getId());

        return taskIds;
    }

    @Override
    public void shutdown() {
        // do nothing
        printCountRunnable.shutdown();
    }

    private void doCreateSchema() {
        DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) schemaConfiguration.findFirst(ConfigurationType.DATABASE);
        String dbname = databaseConfiguration.getName();
        DatabasePrecision precision = databaseConfiguration.getPrecision();
        Connection taosdConnection = TaosdConnectionFactory.build(taosdConfiguration);
        if (isDatabaseMissing(taosdConnection, dbname)) {
            doCreateDatabase(taosdConnection, dbname, precision);
        }

        StableConfiguration stableConfiguration = (StableConfiguration) schemaConfiguration.findFirst(ConfigurationType.STABLE);
        String stableName = stableConfiguration.getName();
        if (isStableMissing(taosdConnection, dbname, stableName)) {
            doCreateSuperTable(taosdConnection, stableConfiguration);
        }
    }

    private void doCreateDatabase(Connection taosdConnection, String dbname, DatabasePrecision precision) {
        try (Statement stmt = taosdConnection.createStatement()) {
            stmt.execute("create database if not exists " + dbname + " precision '" + precision.toString().toLowerCase() + "'");
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
