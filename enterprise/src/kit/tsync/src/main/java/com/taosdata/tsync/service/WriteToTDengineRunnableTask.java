package com.taosdata.tsync.service;

import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WriteToTDengineRunnableTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WriteToTDengineRunnableTask.class);

    private List<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private Connection taosdConnection;
    private Statement statement;
    private int pollingInterval;
    private SchemaMissingStrategy schemaMissing;
    private SchemaConfiguration schemaConfiguration;

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.stream().toArray()));

        while (!Thread.currentThread().isInterrupted()) {
            try {
                doSchemaMissingStrategy();
                statement = taosdConnection.createStatement();
                doWriteToTDengine();
            } catch (SQLException e) {
                logger.error("failed to create statement");
                e.printStackTrace();
            } catch (InterruptedException e) {
                logger.warn(Thread.currentThread().getName() + " is interrupted.");
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void doSchemaMissingStrategy() {
        if (schemaMissing == SchemaMissingStrategy.CREATE) {
            DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) schemaConfiguration.findFirst(ConfigurationType.DATABASE);
            String dbname = databaseConfiguration.getName();
            if (isDatabaseMissing(dbname)) {
                doCreateDatabase(dbname);
            }

            StableConfiguration stableConfiguration = (StableConfiguration) schemaConfiguration.findFirst(ConfigurationType.STABLE);
            String stableName = stableConfiguration.getName();
            if (isStableMissing(dbname, stableName)) {
                doCreateSuperTable(stableConfiguration);
            }
        }
    }

    private void doCreateDatabase(String dbname) {
        try (Statement stmt = taosdConnection.createStatement()) {
            stmt.execute("create database if not exists " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isDatabaseMissing(String dbname) {
        return !showDatabases().contains(dbname);
    }

    private boolean isStableMissing(String dbname, String stableName) {
        return !showStables(dbname).contains(stableName);
    }

    // create table xxx (xx xx, xx xx, xx xx) tags(xx xx, xx xx)
    private void doCreateSuperTable(StableConfiguration stableConfiguration) {
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

    private List<String> showDatabases() {
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

    private List<String> showStables(String database) {
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


    private void doWriteToTDengine() throws Exception {
        while (true) {
            for (int partitionId : partitionsToWrite) {
                consumer.assign(topic, partitionId);
                List<ConsumerRecord> records = consumer.poll();
                for (ConsumerRecord record : records) {
                    final String topic = record.topic();
                    final int partition = record.partition();
                    final long offset = record.offset();
                    String message = new String(record.value(), "UTF-8");
                    logger.trace(String.format("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, message));
                    tryExecuteSQL(message);
                }
            }
            TimeUnit.MILLISECONDS.sleep(pollingInterval);
        }
    }

    public void tryExecuteSQL(String sql) {
        try {
            logger.trace("execute sql >>> " + sql);
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //setter
    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(TQueueConsumer consumer) {
        this.consumer = consumer;
    }

    public void setTaosdConnection(Connection taosdConnection) {
        this.taosdConnection = taosdConnection;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public void setSchemaMissing(SchemaMissingStrategy schemaMissing) {
        this.schemaMissing = schemaMissing;
    }

    public void setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        this.schemaConfiguration = schemaConfiguration;
    }
}
