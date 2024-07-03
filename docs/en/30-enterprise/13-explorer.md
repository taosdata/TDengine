---
toc_max_heading_level: 4
title: Visual Management
---

## Introduction

In order to make it easier for Enterprise Edition users to use and manage the database, TDengine 3.0 Enterprise Edition provides a new visualization component taosExplorer. Users are able to easily manage the lifecycle of the elements of the database management system (databases, super-tables, sub-tables), execute queries, monitor the status of the system, manage users and authorizations, perform data backup and recovery, synchronize data with other clusters, export data, and manage topics and streaming calculations.


## Deployment

See [Installation and Deployment](../../get-started)

## Login

On the login page of TDengine management system, enter the correct user name and password, and then click the Login button to log in.

Description:
- The user here needs to be created in the connected TDengine. The default username and password for TDengine is `root/taosdata`.
- When you create a user in TDengine, the default value of the user's SYSINFO attribute will be set to 1, which means that the user can view the system information, and only the user with the SYSINFO attribute of 1 can log in the TDengine management system normally.

## Dashboard

taosExplorer has a simple built-in dashboard displaying the following cluster information, which can be enabled by clicking on "Panels" in the list of features on the left.

- The default dashboard returns the wizard that corresponds to Grafana's installation and configuration.
- Grafana-configured dashboards are redirected to the corresponding configuration address when clicking on a 'panel' (the address is derived from the return value of the /profile interface)

## Data In

By clicking "Data In" in the function list, you can configure different types of data sources, including TDengine Subscription, PI, OPC-UA, OPC-DA, InfluxDB, MQTT, Kafka, CSV, etc., to write their data to the TDengine cluster that is currently being managed.

In the task list of the Data Source page, the following 4 operations can be made to every task: view、edit, delete and duplicate. Through the duplication operation, a new task can be created easily based on an existing task. After submitting the task, the metrics of the task can be obtained by clicking the "View" button on the task list.

For more information about how to use different data sources, see [Data In](../datain).

### Data sources

#### Monitor task running status

After submitting the task, you can go back to the data source page to view the task status. The task is first added to the execution queue and will start running later. such as Kafka:

![./pic/datain-01](./pic/datain-01.png)

After the task starts running, you can click the **View** button to monitor the dynamic statistics of the task.

![./pic/datain-02](./pic/datain-02.png)

You can also click the **View** button on the right to view the task details.

![./pic/datain-03](./pic/datain-03.png)

#### Create New Agent

1. Click the "Create New Agent" button, download/install the agent according to the document prompts, and click "Next" after confirming the successful installation;

![./pic/agent-01.png](./pic/agent-01.png)

2. Enter the agent name in the input box, such as test_agent, and click "Next";

![./pic/agent-02.png](./pic/agent-02.png)

3. Configure the agent.toml file as prompted and click “Next”；

![./pic/agent-03.png](./pic/agent-03.png)

4. Run the agent according to the document prompts, click "Check whether the agent connection is normal", if the return success, the agent configuration is successful. If the return fails, check the agent log as prompted.

![./pic/agent-04.png](./pic/agent-04.png)

## Data Explorer

You can create and delete databases, create and delete super tables and sub-tables, execute SQL statements, and view the results of SQL statements by clicking the "Data Browser" entry in the function list. In addition, the Super Administrator has administrative rights to the database, a feature not available to other users.As shown in the picture below:

![explorer-01-explorer-entry.jpeg](./pic/explorer-01-explorer-entry.jpeg)

### Create Database

Here, by creating a database, to familiarize yourself with the functions and operations of the data browser page, then look at two ways to create a database:

1. Click the + sign in the figure to jump to the page of creating data database, and click the Create button, as shown in the following figure:

Step 1 Click the + sign;
![explorer-02-createDbentry.jpeg](./pic/explorer-02-createDbentry.jpeg)

Step 2 Fill in the database name and required database configuration parameters. The configuration parameters are classified and folded, and click to expand;
![explorer-03-createDbPage.jpeg](./pic/explorer-03-createDbPage.jpeg)
![explorer-04-createDbPage2.jpeg](./pic/explorer-04-createDbPage2.jpeg)

Step 3 After clicking the "Create" button in Step 3, the database name appears on the left side of the following figure, and the database is successfully created.
![explorer-05-createDbtest01.jpeg](./pic/explorer-05-createDbtest01.jpeg)

2. By data Sql statement in sql editor, click "Run" button, as shown below:

Step 1 Enter the sql statement;
![explorer-06-sqlCreateDb.jpeg](./pic/explorer-06-sqlCreateDb.jpeg)

Step 2 Click the "Run" button, test02 appears on the left, the database is successfully created.
![explorer-07-createDbtest02.jpeg](./pic/explorer-07-createDbtest02.jpeg)

Since creating, modifying, and deleting a super table, creating a table, and creating a child table are consistent in behavior, let's take creating a super table as an example:

### Create a super table

Step 1 Move the mouse to STables, click the + sign, the Create super table tab appears;
![explorer-08-createStbEntry.jpeg](./pic/explorer-08-createStbEntry.jpeg)

Step 2 Fill in the super form information, click "Create" button;
![explorer-09-createStbPage.jpeg](./pic/explorer-09-createStbPage.jpeg)

Step 3 Click Stables to show the super table name just filled in, which proves that the creation is successful.
![explorer-10-createStbSucc.jpeg](./pic/explorer-10-createStbSucc.jpeg)

### View super table

Put the mouse on the super table to be viewed, and the icon as shown in the following picture appears. Click the "eye icon" to view the information of the super table
![explorer-11-viewStableEntry.jpeg](./pic/explorer-11-viewStableEntry.jpeg)
![explorer-12-viewStableInfo.jpeg](./pic/explorer-12-viewStableInfo.jpeg)

### Modify super table

Put the mouse on the super table to be edited, and the icon as shown in the following picture appears. Click "Edit Icon" to modify the information of the super table
![explorer-13-editStableEntry.jpeg](./pic/explorer-13-editStableEntry.jpeg)

### Delete super table

Put the mouse on the super table to be deleted, and the icon as shown in the picture below will appear. Click "Delete icon" to delete the super table
![explorer-15-delStb.jpeg](./pic/explorer-15-delStb.jpeg)

### Sql editor to use

When entering multiple statements, you can select the statement you want to refer to, or comment the statement (shortcut key Control-/ Command-/), and then click Execute
![explorer-16-sqlEditor.jpeg](pic/explorer-16-sqlEditor.jpeg)

## Stream

With Explorer, you can easily manage your streams to take advantage of the streaming capabilities provided by TDengine.
Click "Streaming Calculation" in the left navigation bar to jump to the Streaming Calculation Configuration Management page.
You can create streams in two ways: the Stream Calculation Wizard and custom SQL statements. Currently, the grouping feature is not supported when creating a flow through the Flow Calculation Wizard. When creating streams via custom SQL, you need to understand the syntax of the stream computation SQL statement provided by TDengine and ensure that it is correct.

![stream-01-stream-01-streamEntry.jpeg](./pic/stream-01-streamEntry.jpeg)

### Create a Stream

![stream-02-createStreamEntry.jpeg](./pic/stream-02-createStreamEntry.jpeg)
1. Stream Processing Wizard

Step 1 Fill in the information needed to create a stream calculation, click "Create" button;

![stream-03-createStreamWizard.jpeg](./pic/stream-03-createStreamWizard.jpeg)
![stream-04-createStreamWizard.jpeg](./pic/stream-04-createStreamWizard.jpeg)

Step 2 The following records appear on the page, which proves that the creation is successful.
![stream-05-createStreamSucc1.jpeg](./pic/stream-05-createStreamSucc1.jpeg)

2. Building streams with SQL statements

Step 1 Switch to the SQL page, enter the Create Stream Compute sql directly, and click the "Create" button.
![stream-06-createStreamSql.jpeg](./pic/stream-06-createStreamSql.jpeg)

Step 2 The following records appear on the page, which proves that the creation is successful.
![stream-07-createStreamSucc2.jpeg](./pic/stream-07-createStreamSucc2.jpeg)

## Data Subscription
In this section, you will learn how to create topics and share them with other users in a TDengine cluster, as well as how to view consumer information for a topic.
With Explorer, you can easily manage your data subscriptions to take advantage of the data subscription capabilities offered by TDengine.
Click "Data Subscription" in the left navigation bar to jump to the data subscription configuration management page.

![topic-01-dataSubscription.jpeg](pic/topic-01-dataSubscription.jpeg)

### Create a Topic

![topic-02-addTopic.jpeg](./pic/topic-02-addTopic.jpeg)

1. Data Subscription Processing Wizard

Step 1 Fill in the information needed to add a new theme, click "Create" button;
![topic-03-addTopicWizard.jpeg](./pic/topic-03-addTopicWizard.jpeg)

Step 2 The following records appear on the page, which proves that the creation is successful.
![topic-05-05addTopicSucc1.jpeg](./pic/topic-05addTopicSucc1.jpeg)

2.  Building Topic with SQL statements

Step 1 Switch to the SQL page, directly enter the new topic sql, and click the "Create" button.
![topic-06-addTopicSql.jpeg](./pic/topic-06-addTopicSql.jpeg)

Step 2 The following records appear on the page, which proves that the creation is successful.
![topic-07-addTopicsSucc2.jpeg](./pic/topic-07-addTopicsSucc2.jpeg)

### Share a Topic

On the Share Topic tab, in the Topic drop-down list, select the topic you want to share;
Click the "Add users who can consume this topic" button, and then select the corresponding user in the "Username" drop-down list, and then click "Add" to share this topic then click "Add" to share the topic with this user.

![topic-08-共享主题.jpeg](./pic/topic-08-shareTopic.jpeg)

### View Consumer Groups
- Shared topics can be consumed by executing the "Full Example" described in the "Sample Code" in the next section.  
- On the "Consumer" tab, information about the consumer can be viewed.  
![topic-10-consumer.jpeg](./pic/topic-10-consumer.jpeg)

## Sample Code

- In the Sample Code tab, in the Theme drop-down list, select the appropriate theme;  
- Choose a language you are familiar with, and then you can read and use this part of the sample code to "create consumption", "subscribe to the theme", by executing the "full example" in the program in the "Full Example" to consume shared topics  
![topic-09-sample.jpeg](./pic/topic-09-sample.jpeg)

## System Administration

By clicking on the "System Administration" portal in the function list, you can create users, authorize access to users, and delete users. It is also capable of backing up and restoring the data in the currently managed cluster. You can also configure a remote TDengine address for data synchronization. Cluster and license information as well as proxy information is also provided for viewing. The system administration menu can only be seen by the root user.

### User Management

After clicking "System Administration", you will be taken to the "Users" tab by default.
In the user list, you can view the existing users in the system and their creation time, and you can enable, disable, edit (including changing passwords, database read/write permissions, etc.), delete and other operations on the users.
![management-01-systemEntry.jpeg](./pic/management-01-systemEntry.jpeg)

Step 1 Click the "+ Add" button on the top right of the user list to open the "Add User" dialog box, fill in the information of the new user, and click the "Confirm" button:
![management-02-addUser.jpeg](./pic/management-02-addUser.jpeg)

Step 2 View the new users
![management-03-addUserSucc.jpeg](./pic/management-02-addUserSucc.jpeg)
#### Import Users/Privileges
Click the Import button, and the information of the import user/permission form will pop up. Click OK to submit the form

- Service: imports the service from the specified cluster. The service address is the taosAdapter address, for example, http://127.0.0.1:6041
- Password: specifies the root password of the source cluster
- Items:
  - User & Password: (contains basic user information such as sysinfo/super)
  - Privileges
  - Hosts Whitelist

![management-01-importInfo.jpeg](./pic/management-01-importInfo.jpeg)

## Data Backup and Restoration

You can back up the data in the currently connected TDengine cluster to one or more local files from which you can later perform data recovery. This section describes the specific steps for data backup and recovery.

### Backup data to a local file

1. Enter the system management page, click [Backup] to enter the data backup page, and click [Add Backup] in the upper right corner.
![management-04-backupEntry.jpeg](./pic/management-04-backupEntry.jpeg)

2. Three parameters can be configured in the Data Backup Configuration page:
  - Backup Cycle: Required, configure the time interval for each data backup, you can select daily, every 7 days, every 30 days through the drop-down box to perform a data backup, after the configuration, it will start a data backup task at 0:00 of the corresponding backup cycle;
  - Database: required, configure the name of the database to be backed up (the wal_retention_period parameter of the database should be greater than 0);
  - Directory: Required, configure to back up the data to the specified path in the environment where taosX is running, such as /root/data_backup;
![management-05-backupModal.jpeg](./pic/management-04-backupModal.jpeg)

3. Click [OK] to create a data backup task.

### Recovering from local files

After completing the creation of a data backup task, click [Data Recovery] on the right side of the corresponding data backup task on the page to restore the data that has been backed up to the specified path to the current TDengine.

### Data Replication

Synchronize data between databases, from DB1 to DB2.

Step 1 Go to the system management page, click "Data synchronization page" to enter the data replication page, click "Add New Replication" in the upper right corner.
![management-10-replicationEntry.jpeg](./pic/management-10-replicationEntry.jpeg)

Step 2 Set parameters on the data synchronization page
![management-11-replicationModal.jpeg](./pic/management-11-replicationModal.jpeg)

Step 3 Click "Confirm" to create a data synchronization task.

### Cluster

After clicking the "Cluster" tab, you can view the status, creation time and other information of DNodes, MNodes and QNodes, and you can add and delete the above nodes.
![management-06-cluster.jpeg](./pic/management-06-cluster.jpeg)

### License Management

In the "License" tab of "Management", to make it easier to activate TDengine Enterprise, users can check the cluster ID of TDengine on this page directly.

Due to the restructuring of license in TDengine 3.2.3.0 and later versions, as well as some modifications made to Explorer, the display of this page may vary on different TDengine versions. The following will introduce it separately:

#### TDengine 3.2.3.0 and later versions

By clicking on the "license" tab, you can view the license information for the system and each connector.
![management-12-licenseNew.jpeg](./pic/management-12-licenseNew.jpeg)

Click on the "Activate License" button located in the upper right corner of the "License" tab, enter the "Activation Code" and click on the "OK" button to activate the license, the activation code should be obtained by contacting TDengine Customer Success team.
![management-13-activationCodeNew.jpeg](./pic/management-13-activationCodeNew.jpeg)

#### TDengine version 3.0 before 3.2.3.0

By clicking on the "license" tab, you can view the license information for the system and each connector.
![management-07-许可证.jpeg](./pic/management-07-license.jpeg)

Click on the "Activate License" button located in the upper right corner of the "License" tab, enter the "Activation Code" and "Connector Activation Code" and click on the "OK" button to activate the license, the activation code should be obtained by contacting TDengine Customer Success team.
![management-08-activationCode.jpeg](./pic/management-08-activationCode.jpeg)

### Audit Management

After clicking the "Audit" TAB, you can view the operation database table and login information of each user.
![management-09-audit.jpeg](./pic/management-09-audit.jpeg)
