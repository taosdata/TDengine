# TDinsight for taosX - A Monitoring Solution For [taosX] with [Grafana]

TaosX is a zero-code platform for data access, synchronization, backup, and recovery of TDengine, and is an important feature of TDengine Enterprise Edition. 
TDinsight for taosX is a real-time display plugin used to monitor various components of taosX such as Agent, Connector, and various data source tasks. It is very simple to install and use.

### Adding a TDengine Data Source

Point to the **Configurations** -> **Data Sources** menu, and click the **Add data source** button.

![add datasource](../../assets/howto-add-datasource-button.png)

Search for and select **TDengine**.

![add datasource](../../assets/howto-add-datasource-tdengine.png)

Configure the TDengine datasource.

![datasource config](../../assets/howto-add-datasource.png)

Save and test. It will report 'TDengine Data source is working' in normal circumstances.

![datasource testing](../../assets/howto-add-datasource-test.png)

### Importing dashboard

#### Importing dashboard from datasource confining page.

Click **Dashboard** tab in TDengine datasource confining page.

![import dashboard and config](../../assets/taosX-import.png)

Click the "import" button of `TDinsight for taoX` to import the panel. 
After the import is complete, the full page view of `TDinsight for taoX` is as shown below.

![dashboard](../../assets/TDinsight-taosX-full.png)


## TDinsight for taosX Dashbord Detail

### taosX

![taosx-overview](../../assets/taosX-main.png)

This section includes the basic information of the currently selected taosX instance (from left to right, from top to bottom).

- **CPU Cores**: The number of CPU cores of the machine where taosX is located.
- **Total Memory**: The system memory of the machine where taosX is located.
- **CPU Usage**: The percentage of CPU usage by the taosX process.
- **Memory Usage**: The percentage of memory usage by the taosX process.
- **Uptime**: The running time of taosX.
- **Restart Times**: The number of times taosX has been restarted.
- **CPU Usage**: The percentage of CPU usage by the taosX process, in time series form.
- **Memory Usage**: The percentage of memory usage by the taosX process, in time series form.
- **Running Tasks**: The number of tasks currently being executed.
- **Failed Tasks**: The number of tasks failed.
- **Completed Tasks**: The number of tasks completed.

### Agent

![taosx-agent](../../assets/taosX-Agent.png)

- **CPU Cores**: The number of CPU cores of the machine where the Agent is located.
- **Total Memory**: The system memory of the machine where the Agent is located.
- **CPU Usage**: The percentage of CPU usage by the Agent process.
- **Memory Usage**: The percentage of memory usage by the Agent process.
- **CPU Usage**: The percentage of CPU usage by the Agent process, in time series form.
- **Memory Usage**: The percentage of memory usage by the Agent process, in time series form.

### TDengine Data Subscription

![taosX TDengine Data Subscription](../../assets/taosX-tdengine-subscription.png)

-  **Task Info**: Task information, including task id, name, execution time, total execution time.
-  **VGroup Consumer Progress**：VGroup consumer progress，including update time, task id, topic name, vgroup, offset, and latest.
-  **Messages Number/Bytes per Second**: The messages number per second on the left Y-axis, and the throughput (bytes per second) on the right Y-axis.
-  **Write Raw Fails**: The number of times the writing of raw meta failed.

### OPC-UA

![taosX OPC-UA](../../assets/taosX-opcua.png)

- **Task Info**: Task information, including id, name, execution time, number of rows written, total execution time, and total number of rows written.
- **Inserted Rows Rate**: Insertion rate of rows.
- **Inserted Points Rate**: Insertion rate of points.
- **Processed/Received Batches**: The number of batches processed & the number of data batches received through IPC Stream.
- **Failed Sqls**: The total number of failed INSERT SQL statements during this task run.
- **Connector CPU Percent**: The percentage of CPU usage by the Connector process, in time series form.
- **Connector Memory Percent**: The percentage of memory usage by the Connector process, in time series form.
- **Connector Disk Read Rate**: The disk read speed of the Connector process.
- **Connector Disk Write Rate**: The disk write speed of the Connector process.

The monitoring information of other types of data sources such as CSV, Kafka, etc., is similar to OPC-UA.

### MQTT

![taosX MQTT](../../assets/taosX-MQTT.png)

- **Task Info**: Task information, including id, name, execution time, number of rows written, total execution time, total number of rows written, fetched messages and processed messages.
- **MQTT Written/Fetch Rate**: The ratio of messages written to TDengine and the rate of messages fetched from the MQTT broker, it's usually 1:1.
- **MQTT Consuming Rate**: The rate of messages consumed from the MQTT broker in messages per second and bytes per second.
- **Processed/Received Batches**: The number of batches processed & the number of data batches received from source.
- **Inserted Rate**: The rate of messages inserted into TDengine in rows per second and number of points(calculated by columns x rows) per second.
