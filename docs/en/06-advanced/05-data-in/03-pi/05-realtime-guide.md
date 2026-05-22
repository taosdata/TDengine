---
title: "PI Real-time Data Sync Guide"
sidebar_label: "Real-time Data Sync"
---

This page describes how to use PI real-time tasks to continuously sync real-time data from the PI system to TDengine, covering task configuration, advanced features, best practices, and troubleshooting.

## 1. Overview

PI real-time tasks continuously subscribe to data changes in the PI system and write newly generated data to TDengine in real time. Typical use cases include:

- **Real-time monitoring**: Sync PI real-time data to TDengine to build real-time dashboards leveraging TDengine's high-performance query capabilities
- **Dual-write transition**: Run both systems in parallel during migration from PI to TDengine
- **Data aggregation**: Aggregate real-time data from multiple PI systems into a unified TDengine cluster

## 2. Creating a PI Real-time Task

### 2.1 Basic Steps

1. On the Data In page in Explorer, click **+Add Data Source**
2. In the **Type** dropdown, select **PI**
3. Configure connection information (see [main documentation](./index.md))
4. Configure data model (single/multi-column, see [Model Configuration Reference](./03-csv-reference.md))
5. Configure restart compensation time (see next section)
6. Configure advanced options (see section 4)
7. Submit the task

### 2.2 Restart Compensation Time

**Restart compensation time** is a key parameter for PI real-time tasks. When a task is unexpectedly interrupted and restarted, taosX will automatically backfill data from the interruption time to the current time.

| Parameter | Description |
| --------- | ----------- |
| Restart Compensation Time | Sets the maximum time window for automatic backfill after restart |

**Configuration recommendations**:

- Set based on the maximum data loss duration you can tolerate
- For example, if set to 1 hour and the task was interrupted for 30 minutes before restart, taosX will backfill the 30 minutes of data
- If the interruption duration exceeds the compensation time setting, the excess data needs to be manually recovered through a PI backfill task

![PI Real-time task restart compensation time](../../../assets/pi-system-06-backfill-realtime.png)

## 3. Data Sync Behavior

### 3.1 Single-column Model

- Subscribes to value changes for each PI Point
- When a PI Point's value changes, the new value is written to the corresponding TDengine subtable
- Each PI Point corresponds to one subtable

### 3.2 Multi-column Model

- Subscribes to PI Point attribute changes for all elements under an AF template
- When a PI Point attribute value of an element changes, the corresponding column in the corresponding subtable is updated
- Each AF element corresponds to one subtable

## 4. Multi-column Model Advanced Features

The following advanced options are only available when using **multi-column model real-time tasks**.

### 4.1 Sync New Elements

| Option | Default | Description |
| ------ | ------- | ----------- |
| Sync New Elements | Enabled | When enabled, the PI connector monitors newly added elements under templates and automatically syncs their data |

**Use case**: When new devices/assets are continuously being added under AF templates in the PI system, no need to manually restart the task or modify configuration.

### 4.2 Sync Static Attribute Changes

| Option | Default | Description |
| ------ | ------- | ----------- |
| Sync Static Attribute Changes | Enabled | When enabled, changes to non-PI Point attributes (static attributes) are synced to TDengine TAGs |

**Use case**: When static attributes of AF elements such as description, category, location, etc. may be modified, this keeps TDengine TAGs consistent with PI AF.

### 4.3 Sync Delete Elements

| Option | Default | Description |
| ------ | ------- | ----------- |
| Sync Delete Elements | Enabled | When enabled, the PI connector monitors element deletion events under templates and deletes the corresponding TDengine subtables |

:::warning
When this option is enabled, deleting an element in PI will cause the corresponding subtable in TDengine to be deleted, and **data cannot be recovered**. Use with caution.
:::

### 4.4 Sync Delete Historical Data

| Option | Default | Description |
| ------ | ------- | ----------- |
| Sync Delete Historical Data | Enabled | When enabled, when data at a specific timestamp is deleted in PI, the corresponding column values in TDengine are set to null |

### 4.5 Sync Modify Historical Data

| Option | Default | Description |
| ------ | ------- | ----------- |
| Sync Modify Historical Data | Enabled | When enabled, when historical data is modified in PI, the corresponding data in TDengine is also updated |

**Use case**: PI systems may have data correction operations (e.g., manually modifying anomalous values); enabling this option keeps TDengine data consistent with PI.

## 5. Best Practices

### 5.1 High Availability Deployment

- Recommend using Agent proxy mode deployment (see [Deployment Architecture](./02-deployment-architecture.md)) to separate the connector from taosX
- Configure a reasonable restart compensation time to ensure no data loss after task restart
- Monitor task status through Explorer to detect anomalies promptly

### 5.2 Performance Tuning

| Tuning Item | Recommendation |
| ----------- | -------------- |
| Batch Size | Adjust based on data density; increase appropriately when there are many points with high data frequency |
| Max Read Delay Per Batch | Adjust based on real-time requirements |
| Log Level | Use `info` or `warn` in production; temporarily switch to `debug` for troubleshooting |

### 5.3 Task Monitoring

Through the data source list page in Explorer, you can view:

- Task running status
- Data write rate
- Last sync time
- Error logs

We recommend regularly checking task status to ensure real-time sync is running normally.

## 6. FAQ

### How to compensate for real-time task data loss?

If the real-time task interruption duration exceeds the restart compensation time setting, you can compensate through the following steps:

1. Record the time period of the task interruption
2. Create a PI backfill task with a time range covering the interruption period
3. Run the backfill task to recover data
4. After backfill is complete, the real-time task continues normal operation

### How to troubleshoot excessive real-time sync latency?

1. **Network latency**: Check the network latency from taosX/agent to the PI system
2. **PI system load**: Check the PI Data Archive load
3. **TDengine write bottleneck**: Check the TDengine cluster write performance
4. **Batch size too large**: Reduce the batch size appropriately to lower per-batch processing latency
5. **Log troubleshooting**: Set the log level to `debug` to view detailed processing times

### Do I need to restart the task after adding new points?

- **Multi-column model**: If the **Sync New Elements** option is enabled, no restart is needed
- **Single-column model**: You need to update the model configuration file (add the new point's mapping), then restart the task
- Alternatively, after adding new points in the PI system, re-download the default configuration file and upload it

### Multi-column model TAG values not updating?

Confirm whether the **Sync Static Attribute Changes** option is enabled. If not enabled, TAG values are written only once when the subtable is created and will not be updated afterward.
