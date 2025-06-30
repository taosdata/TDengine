---
title: Grafana
slug: /third-party-tools/visualization/grafana
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/grafana-01.png';
import imgStep02 from '../../assets/grafana-02.png';
import imgStep03 from '../../assets/grafana-03.png';
import imgStep04 from '../../assets/grafana-04.png';
import imgStep05 from '../../assets/grafana-05.png';
import imgStep06 from '../../assets/grafana-06.png';
import imgStep07 from '../../assets/grafana-07.png';
import imgStep08 from '../../assets/grafana-08.png';
import imgStep09 from '../../assets/grafana-09.png';
import imgStep10 from '../../assets/grafana-10.png';
import imgStep11 from '../../assets/grafana-11.png';

## Overview

This document describes how to integrate the TDengine data source with the open-source data visualization system [Grafana](https://www.grafana.com/) to achieve data visualization and build a monitoring and alert system. With the TDengine plugin, you can easily display data from TDengine tables on Grafana dashboards without the need for complex development work.

## Prerequisites

To add the TDengine data source to Grafana normally, the following preparations are needed.

- Grafana service has been deployed and is running normally. TDengine currently supports Grafana version 7.5 and above. It is recommended to use the latest version.  
    **Note**: Ensure that the account starting Grafana has write permissions to its installation directory, otherwise you may not be able to install plugins later.
- TDengine cluster has been deployed and is running normally.
- taosAdapter has been installed and is running normally. For details, please refer to the [taosAdapter user manual](../../../tdengine-reference/components/taosadapter/)

Record the following information:

- TDengine cluster REST API address, such as: `http://tdengine.local:6041`.
- TDengine cluster authentication information, using username and password.

## Install Grafana Plugin and Configure Data Source

<Tabs defaultValue="script">
<TabItem value="script" label="Installation Script">

For users using Grafana version 7.x or configuring with [Grafana Provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/), you can use the installation script on the Grafana server to automatically install the plugin and add the data source Provisioning configuration file.

```shell
bash -c "$(curl -fsSL \
  https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)" -- \
  -a http://localhost:6041 \
  -u root \
  -p taosdata
```

After installation, you need to restart the Grafana service for it to take effect.

Save the script and execute `./install.sh --help` to view detailed help documentation.

</TabItem>
<TabItem value="command" label="Command Line Tool">

Use the [`grafana-cli` command line tool](https://grafana.com/docs/grafana/latest/administration/cli/) to install the plugin. After installation, Grafana needs to be restarted.

On Linux or macOS, run the following command in your terminal:

```shell
grafana-cli --pluginUrl \
      https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip \
      plugins install tdengine-datasource
# with sudo
sudo -u grafana grafana-cli --pluginUrl \
      https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip \
      plugins install tdengine-datasource
```

On Windows, first ensure that the plugin installation directory exists (by default, it is located in the data/plugins subdirectory of your Grafana installation directory). Then, run the following command in the bin directory of the Grafana installation path using an administrator account:

```shell
./grafana-cli.exe --pluginUrl https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip plugins install tdengine-datasource
```

Afterward, users can directly access the Grafana server at `http://localhost:3000` (username/password: admin/admin), and add a data source through `Configuration -> Data Sources`,

Click `Add data source` to enter the new data source page, type TDengine in the search box, then click `select` to choose and you will enter the data source configuration page, modify the configuration according to the default prompts:

- Host: IP address and port number providing REST service in the TDengine cluster, default `http://localhost:6041`
- User: TDengine username.
- Password: TDengine user password.

Click `Save & Test` to test, if successful, it will prompt: `TDengine Data source is working`

</TabItem>

<TabItem value="manual" label="Manual Installation">

Download [tdengine-datasource.zip](https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip) to your local machine and unzip it into the Grafana plugins directory. Example command line download is as follows:

```shell
wget https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip
```

For CentOS 7.2 operating system, unzip the plugin package into the /var/lib/grafana/plugins directory and restart Grafana.

```shell
sudo unzip tdengine-datasource.zip -d /var/lib/grafana/plugins/
```

Afterward, users can directly access the Grafana server at `http://localhost:3000` (username/password: admin/admin), and add a data source through `Configuration -> Data Sources`,

Click `Add data source` to enter the new data source page, type TDengine in the search box, then click `select` to choose and you will enter the data source configuration page, modify the configuration according to the default prompts:

- Host: IP address and port number providing REST service in the TDengine cluster, default `http://localhost:6041`
- User: TDengine username.
- Password: TDengine user password.

Click `Save & Test` to test, if successful, it will prompt: `TDengine Data source is working`

</TabItem>
<TabItem value="container" label="K8s/Docker container">

Refer to [Grafana containerized installation instructions](https://grafana.com/docs/grafana/next/setup-grafana/installation/docker/#install-plugins-in-the-docker-container). Use the following command to start a container and automatically install the TDengine plugin:

```shell
docker run -d \
  -p 3000:3000 \
  --name=grafana \
  -e "GF_INSTALL_PLUGINS=https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip;tdengine-datasource" \
  grafana/grafana
```

Using docker-compose, configure Grafana Provisioning for automated setup, and experience a zero-configuration start with TDengine + Grafana:

1. Save this file as `tdengine.yml`.

    ```yml
    apiVersion: 1
    datasources:
    - name: TDengine
      type: tdengine-datasource
      orgId: 1
      url: "$TDENGINE_API"
      isDefault: true
      secureJsonData:
        url: "$TDENGINE_API"
        basicAuth: "$TDENGINE_BASIC_AUTH"
      version: 1
      editable: true
    ```

2. Save the file as `docker-compose.yml`.

    ```yml
    version: "3.7"

    services:
      tdengine:
        image: tdengine/tdengine:latest
        container_name: tdengine
        hostname: tdengine
        environment:
          TAOS_FQDN: tdengine
          MONITOR_FQDN: tdengine
          EXPLORER_CLUSTER: http://tdengine:6041
          TAOS_KEEPER_TDENGINE_HOST: tdengine
        volumes:
          - tdengine-data:/var/lib/taos/
        ports:
          - 6060:6060
      grafana:
        image: grafana/grafana:latest
        volumes:
          - ./tdengine.yml:/etc/grafana/provisioning/datasources/tdengine.yml
          - grafana-data:/var/lib/grafana
        environment:
          # install tdengine plugin at start
          GF_INSTALL_PLUGINS: "https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip;tdengine-datasource"
          TDENGINE_API: "http://tdengine:6041"
          #printf "$TDENGINE_USER:$TDENGINE_PASSWORD" | base64
          TDENGINE_BASIC_AUTH: "cm9vdDp0YW9zZGF0YQ=="
        ports:
          - 3000:3000

    volumes:
      grafana-data:
      tdengine-data:
    ```

3. Use the docker-compose command to start TDengine + Grafana: `docker-compose up -d`.

Open Grafana [http://localhost:3000](http://localhost:3000), and now you can add Dashboards.

</TabItem>
</Tabs>

:::info

In the following text, we use Grafana v11.0.0 as an example. Other versions may have different features, please refer to [Grafana's official website](https://grafana.com/docs/grafana/latest/).

:::

## Dashboard Usage Guide

This section is organized as follows:

1. Introduce basic knowledge, including Grafana's built-in variables and custom variables, and TDengine's special syntax support for time-series queries.
2. Explain how to use the TDengine data source in Grafana to create Dashboards, then provide the special syntax for time-series queries and how to group display data.
3. Since the configured Dashboard will periodically query TDengine to refresh the display, improper SQL writing can cause serious performance issues, so we provide performance optimization suggestions.
4. Finally, we use the TDengine monitoring panel TDinsight as an example to demonstrate how to import the Dashboards we provide.

### Grafana Built-in Variables and Custom Variables

The Variable feature in Grafana is very powerful and can be used in Dashboard queries, panel titles, tags, etc., to create more dynamic and interactive Dashboards, enhancing user experience and efficiency.

The main functions and features of variables include:

- Dynamic data querying: Variables can be used in query statements, allowing users to dynamically change query conditions by selecting different variable values, thus viewing different data views. This is very useful for scenarios that require dynamically displaying data based on user input.

- Improved reusability: By defining variables, the same configuration or query logic can be reused in multiple places without having to rewrite the same code. This makes maintaining and updating Dashboards simpler and more efficient.

- Flexible configuration options: Variables offer a variety of configuration options, such as predefined static value lists, dynamically querying values from data sources, regular expression filtering, etc., making the application of variables more flexible and powerful.

Grafana provides both built-in and custom variables, which can be referenced when writing SQL as `$variableName`, where `variableName` is the name of the variable. For other referencing methods, please refer to [Referencing Methods](https://grafana.com/docs/grafana/latest/dashboards/variables/variable-syntax/).

#### Built-in Variables

Grafana has built-in variables such as `from`, `to`, and `interval`, all derived from the Grafana plugin panel. Their meanings are as follows:

- `from` is the start time of the query range
- `to` is the end time of the query range
- `interval` is the window split interval

For each query, it is recommended to set the start and end time of the query range, which can effectively reduce the amount of data scanned by the TDengine server during query execution. `interval` is the size of the window split, and in Grafana version 11, it is calculated based on the time interval and the number of returned points.

In addition to the three common variables mentioned above, Grafana also provides variables such as `__timezone`, `__org`, `__user`, etc. For more details, please refer to [Built-in Variables](https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/#global-variables).

#### Custom Variables

We can add custom variables in the Dashboard. The usage of custom variables is no different from built-in variables; they are referenced in SQL with `$variableName`.
Custom variables support multiple types, including common types such as `Query` (query), `Constant` (constant), `Interval` (interval), `Data source` (data source), etc.
Custom variables can reference other custom variables, for example, one variable represents a region, and another variable can reference the value of the region to query devices in that region.

##### Adding a Query Type Variable

In the Dashboard configuration, select **Variables**, then click **New variable**:

1. In the "Name" field, enter your variable name, here we set the variable name as `selected_groups`.
2. In the **Select variable type** dropdown menu, select "Query" (query).
Depending on the selected variable type, configure the corresponding options. For example, if you choose "Query", you need to specify the data source and the query statement to obtain the variable values. Here, we take smart meters as an example, set the query type, select the data source, and configure the SQL as `select distinct(groupid) from power.meters where groupid < 3 and ts > $from and ts < $to;`
3. After clicking **Run Query** at the bottom, you can see the variable values generated based on your configuration in the "Preview of values" section.
4. Other configurations are not detailed here; after completing the configuration, click the **Apply** button at the bottom of the page, then click **Save dashboard** in the upper right corner to save.

After completing the above steps, we have successfully added a new custom variable `$selected_groups` in the Dashboard. We can later reference this variable in the Dashboard's queries through `$selected_groups`.

We can also add another custom variable to reference this `selected_groups` variable, such as adding a query variable named `tbname_max_current`, with its SQL as `select tbname from power.meters where groupid = $selected_groups and ts > $from and ts < $to;`

##### Adding an Interval Type Variable

We can customize the time window interval to better fit business needs.

1. In the "Name" field, enter the variable name as `interval`.
2. In the **Select variable type** dropdown menu, select "Interval" (interval).
3. In the **Interval options** enter `1s,2s,5s,10s,15s,30s,1m`.
4. Other configurations are not detailed here; after completing the configuration, click the **Apply** button at the bottom of the page, then click **Save dashboard** in the upper right corner to save.

After completing the above steps, we have successfully added a new custom variable `$interval` in the Dashboard. We can later reference this variable in the Dashboard's queries through `$interval`.

:::note

When custom variables and Grafana built-in variables have the same name, custom variables are referenced preferentially.

:::

### TDengine Time-Series Query Support

On top of supporting standard SQL, TDengine also offers a series of special query syntaxes that meet the needs of time-series business scenarios, greatly facilitating the development of applications for time series scenarios.

- The `partition by` clause can split data by certain dimensions, then perform a series of calculations within the split data space, often replacing `group by`.
- The `interval` clause is used to generate windows of equal time periods.
- The `fill` statement specifies the filling mode for missing data in a window interval.
- `Timestamp pseudocolumns` If you need to output the time window information corresponding to the aggregation results in the results, you need to use timestamp-related pseudo-columns in the SELECT clause: window start time (_wstart), window end time (_wend), etc.

Detailed introduction to the above features can be found at [Distinguished Queries](../../../tdengine-reference/sql-manual/time-series-extensions/).

### Creating a Dashboard

With the foundational knowledge from earlier, we can configure a time-series data display Dashboard based on the TDengine data source.  
Create a Dashboard on the Grafana main interface, click on **Add Query** to enter the panel query page:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

As shown in the image above, select the `TDengine` data source in "Query", and enter the corresponding SQL in the query box below. Continuing with the example of smart meters, to display a beautiful curve, **virtual data is used here**.

#### Time-Series Data Display

Suppose we want to query the average current size over a period of time, with the time window divided by `$interval`, and fill with null if data is missing in any time window.

- "INPUT SQL": Enter the query statement (the result set of this SQL statement should be two columns and multiple rows), here enter: `select _wstart as ts, avg(current) as current from power.meters where groupid in ($selected_groups) and ts > $from and ts < $to interval($interval) fill(null)`, where from, to, and interval are Grafana built-in variables, and selected_groups is a custom variable.
- "ALIAS BY": You can set an alias for the current query.
- "GENERATE SQL": Clicking this button will automatically replace the corresponding variables and generate the final execution statement.

In the custom variables at the top, if the value of `selected_groups` is set to 1, then querying the average value changes of all devices' current in the `meters` supertable with `groupid` 1 is shown in the following image:

<figure>
<Image img={imgStep02} alt=""/>
</figure>

:::note

Since the REST interface is stateless, you cannot use the `use db` statement to switch databases. In the Grafana plugin, SQL statements can specify the database using \<db_name>.\<table_name>.

:::

#### Time-Series Data Group Display

Suppose we want to query the average current size over a period of time and display it grouped by `groupid`, we can modify the previous SQL to `select _wstart as ts, groupid, avg(current) as current from power.meters where ts > $from and ts < $to partition by groupid interval($interval) fill(null)`

- "Group by column(s)": Comma-separated `group by` or `partition by` column names in **half-width** commas. If it is a `group by` or `partition by` query statement, set the "Group by" column to display multidimensional data. Here, set the "Group by" column name as `groupid` to display data grouped by `groupid`.
- "Group By Format": Legend format for multidimensional data in `Group by` or `Partition by` scenarios. For example, in the above INPUT SQL, set the "Group By Format" to `groupid-{{groupid}}`, and the displayed legend name will be the formatted group name.

After completing the settings, the display grouped by `groupid` is shown in the following image:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

> For information on how to use Grafana to create corresponding monitoring interfaces and more about using Grafana, please refer to the official [documentation](https://grafana.com/docs/) of Grafana.

### Performance Optimization Suggestions

- **Add a time range to all queries**, in time-series databases, if a time range is not specified in the query, it will lead to table scanning and poor performance. A common SQL syntax is `select column_name from db.table where ts > $from and ts < $to;`
- For queries of the latest state type, we generally recommend **enabling cache when creating the database** (`CACHEMODEL` set to last_row or both), a common SQL syntax is `select last(column_name) from db.table where ts > $from and ts < $to;`

### Import Dashboard

On the data source configuration page, you can import the TDinsight panel for this data source, serving as a monitoring visualization tool for the TDengine cluster. If the TDengine server is version 3.0, please select `TDinsight for 3.x` for import. Note that TDinsight for 3.x requires running and configuring taoskeeper.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

The Dashboard compatible with TDengine 2.* has been released on Grafana: [Dashboard 15167 - TDinsight](https://grafana.com/grafana/dashboards/15167)).

Other panels using TDengine as a data source can be [searched here](https://grafana.com/grafana/dashboards/?dataSource=tdengine-datasource). Below is a non-exhaustive list:

- [15146](https://grafana.com/grafana/dashboards/15146): Monitoring multiple TDengine clusters
- [15155](https://grafana.com/grafana/dashboards/15155): TDengine alert example
- [15167](https://grafana.com/grafana/dashboards/15167): TDinsight
- [16388](https://grafana.com/grafana/dashboards/16388): Display of node information collected by Telegraf

## Alert Configuration

The TDengine Grafana plugin supports alerts. To configure alerts, follow these steps:

1. Configure contact points: Set up notification channels, including DingDing, Email, Slack, WebHook, Prometheus Alertmanager, etc.
2. Configure notification policies: Set up routing of alerts to specific channels, as well as notification timing and repeat frequency
3. Configure alert rules: Set up detailed alert rules  
    3.1 Configure alert name  
    3.2 Configure queries and alert trigger conditions  
    3.3 Configure rule evaluation strategy  
    3.4 Configure tags and alert channels  
    3.5 Configure notification content  

### Alert Configuration Interface Introduction

In Grafana 11, the alert interface has 6 tabs, namely "Alert rules", "Contact points", "Notification policies", "Silences", "Groups", and "Settings".

- "Alert rules" displays and configures alert rules
- "Contact points" includes notification channels such as DingDing, Email, Slack, WebHook, Prometheus Alertmanager, etc.
- "Notification policies" sets up routing of alerts to specific channels, as well as notification timing and repeat frequency
- "Silences" configures silent periods for alerts
- "Groups" displays grouped alerts after they are triggered
- "Settings" allows modifying alert configurations via JSON

### Configuring Contact Points

This section uses email and Lark as examples to configure contact points.

#### Configuring Email Contact Points

Add the SMTP/Emailing and Alerting modules to the configuration file of the Grafana service. (For Linux systems, the configuration file is usually located at `/etc/grafana/grafana.ini`)

Add the following content to the configuration file:

```ini
#################################### SMTP / Emailing ##########################
[smtp]
enabled = true
host = smtp.qq.com:465      #Email used
user = receiver@foxmail.com
password = ***********      #Use mail authorization code
skip_verify = true
from_address = sender@foxmail.com
```

Then restart the Grafana service (for Linux systems, execute `systemctl restart grafana-server.service`) to complete the addition.

On the Grafana page, go to "Home" -> "Alerting" -> "Contact points" and create a new contact point  
"Name": Email Contact Point  
"Integration": Select the contact type, here choose Email, fill in the email receiving address, and save the contact point after completion  

<figure>
<Image img={imgStep05} alt=""/>
</figure>

### Configure Notification Policies

After configuring the contact points, you can see there is a Default Policy

<figure>
<Image img={imgStep06} alt=""/>
</figure>

Click on the right side "..." -> "Edit", then edit the default notification policy, a configuration window pops up:

<figure>
<Image img={imgStep07} alt=""/>
</figure>

Then configure the following parameters:

- "Group wait": The wait time before sending the first alert.
- "Group interval": The wait time to send the next batch of new alerts for the group after the first alert.
- "Repeat interval": The wait time to resend the alert after a successful alert.

### Configure Alert Rules  

Taking the configuration of smart meter alerts as an example, the configuration of alert rules mainly includes alert name, query and alert trigger conditions, rule evaluation strategy, tags, alert channels, and notification copy.

#### Configure Alert Name

In the panel where you need to configure the alert, select "Edit" -> "Alert" -> "New alert rule".

"Enter alert rule name" (input alert rule name): Here, enter `power meters alert` as an example

#### Configure Query and Alert Trigger Conditions

In "Define query and alert condition" configure the alert rule.

1. Choose data source: `TDengine Datasource`  
2. Query statement:

    ```sql
    select _wstart as ts, groupid, avg(current) as current from power.meters where ts > $from and ts < $to partition by groupid interval($interval) fill(null)
    ```

3. Set "Expression" (expression): `Threshold is above 100`  
4. Click [Set as alert condition]
5. "Preview": View the results of the set rules  

After setting, you can see the image displayed below:

<figure>
<Image img={imgStep08} alt=""/>
</figure>

Grafana's "Expression" (expression) supports various operations and calculations on data, which are divided into:

1. "Reduce": Aggregates the values of a time-series within a selected time range into a single value  
    1.1 "Function" is used to set the aggregation method, supporting Min, Max, Last, Mean, Sum, and Count.  
    1.2 "Mode" supports the following three:  
        - "Strict": If no data is queried, the data will be assigned as NaN.  
        - "Drop Non-numeric Value": Remove illegal data results.  
        - "Replace Non-numeric Value": If it is illegal data, replace it with a fixed value.  
2. "Threshold": Checks whether the time-series data meets the threshold judgment conditions. Returns 0 when the condition is false, and 1 when true. Supports the following methods:
    - Is above (x > y)
    - Is below (x \< y)
    - Is within range (x > y1 AND x \< y2)
    - Is outside range (x \< y1 AND x > y2)
3. "Math": Performs mathematical operations on the data of the time-series.
4. "Resample": Changes the timestamps in each time-series to have a consistent interval, allowing mathematical operations to be performed between them.
5. "Classic condition (legacy)": Configurable multiple logical conditions to determine whether to trigger an alert.

As shown in the screenshot above, here we set the maximum value to trigger an alarm when it exceeds 100.

#### Configure Rule Evaluation Strategy

<figure>
<Image img={imgStep09} alt=""/>
</figure>

Complete the following configurations:  

- "Folder": Set the directory to which the alert rule belongs.
- "Evaluation group": Set the evaluation group for the alert rule. "Evaluation group" can either select an existing group or create a new one, where you can set the group name and evaluation interval.
- "Pending period": After the threshold of the alert rule is triggered, how long the abnormal value continues can trigger an alarm, and a reasonable setting can avoid false alarms.

#### Configure Labels and Alert Channels

<figure>
<Image img={imgStep10} alt=""/>
</figure>

Complete the following configurations:  

- "Labels" adds labels to the rule for searching, silencing, or routing to notification policies.
- "Contact point" selects a contact point to notify through the set contact point when an alert occurs.

#### Configure Notification Text

<figure>
<Image img={imgStep11} alt=""/>
</figure>

Set "Summary" and "Description", and if an alert is triggered, you will receive a notification.
