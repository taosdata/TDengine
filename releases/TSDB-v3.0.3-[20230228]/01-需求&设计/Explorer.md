# Explorer

- Owned by:@霍琳贺 @姜亚利
Explorer is a unified visual tool for TDengine database architects, developers, and DBAs. Explorer provides data modeling, SQL development, and comprehensive administration tools for server configuration, user administration, backup-restore, subscription and much more.

## 1. Install

Explorer works as a standalone service.
First, you should ensure that `taosd` and `taosadapter` services have already been started.
Second, if you want to use features including backup/restore and data replication(or data in), you should use `taosx` service. Save the systemd unit file to x, and start it with `systemctl start taosx`.
```toml
[Unit]
Description=taosX - Data Replication and Streaming Data Integration Toolset.
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/taosx
Restart=always

[Install]
WantedBy=multi-user.target

```

Next, if you prefer systemd, save the systemd unit to `/etc/systemd/system/taos-explorer.service` , then start it with `systemctl start taos-explorer`. Otherwise, you can directly run `taos-explorer` without additional user permissions.

```toml
[Unit]
Description=Explorer for TDengine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/taos-explorer
Restart=always

[Install]
WantedBy=multi-user.target

```

### 1.1 Configuration

Explorer exposes port `6060` by default. Change it from `/etc/taos/explorer.toml` to a user-defined port.
Example configuration file:
```toml
listen = "0.0.0.0:6060"
log_level = "info"
x_api = "http://localhost:6050"
cluster = "http://localhost:6041"
```

- `listen`: use "<ip>:<port>" format to listen on a specific network and port.
- `log_level`: choose a log level by string: `error`, `warn`, `info`, `debug`, `trace`.
- `x_api`: the taosx REST API endpoint, such as `http://localhost:6050`, default is none.
- `cluster`: the cluster url to connect, you can use REST API endpoint starts with `http://` or `https://` like  `http://localhost:6041`.

## 2. Usage

### 2.1 Login

Users could login to specified cluster, with its REST API url (with 6041(`taosadapter`port).
Login with username and password, then explore others.

You need to input the full address of the cluster (http or https), which is the backend interface call address for the whole platform, followed by the username and password. Note that, here the cluster address is relative to the web-browser client. When using taosX service, the cluster address should be directly accessible by the server running taosX.
![](./images/img_boxcnTikjvGccljz7R7klsXobFf.png)

### 2.2 Dashboard

The interaction has changed and  will supplement it later.
Enhanced dashboard using TDinsight will be added soon.

### 2.3 Explorer

You can view databases in the current cluster like file explorer.
The left panel is a tree-view of databases-(stables-subtables | tables) display. Click on a database to see its properties in the right panel.
The right panel has a SQL editor. You can edit and run any SQL with the editor. Query results will be displayed next to the editor on the downside. You can use the results to plot with *Charts*.
The Explorer module can create databases, super tables and ordinary tables, and display the relationship between databases, super tables and ordinary tables through a tree structure; the SQL module can input the corresponding SQL statement to query the corresponding table data, the display module can display the query results in Grid and Chart, and the user can mark the query SQL statement and display the log according to the needs.
![](./images/img_boxcnn87IGjuidfhjng1pvmXIub.png)

![](./images/img_boxcnYLwQ6XjC2SbE9DPzUb2tFf.png)

![](./images/img_boxcne2VF66f343Y1Wcq0nATzBg.png)

![](./images/img_boxcnz7Vd1vchtQxWRCxWG0cKee.png)

The super table query is as follows:
1. Enter the corresponding SQL statement and execute the *R**un* operation.
2. The panel below will display the query results in *Grid* format by default.
3. You can manually switch tabs to display the corresponding results.
4. You can save the corresponding SQL statement to Favorites by marking the *Favorite* function, and you can copy, share and delete the corresponding favorite statements.
5. Display the history of running SQL statements.

![](./images/img_boxcnt30FhNcGMPP4rlmmBcvlHf.png)

![](./images/img_boxcnQrXuCAeiGQkKLNOfqVjOGh.png)

![](./images/img_boxcnWqh194zAyb7aoYChlng9zb.png)

![](./images/img_boxcnL3Nm04qo2qNrtazEdywFSb.png)

6.You can also customize the length(varchar && nchar).
![](./images/img_boxcnCyvV1QCGLHmLYTY56G2BBb.png)

![](./images/img_boxcnCal2hF6lRRUAH8b1v8oRYd.png)

Creating ordinary tables is similar to creating super tables, and you can follow the above steps to create ordinary tables.

### 2.4 Data In

Data In is divided into three modules: Data Collection Agents, Data Source, and CSV.

##### 2.4.0.1 Data Collection Agents

Data Collection Agents mainly displays details of third-party agents.
![](./images/img_boxcnIZ2p1lV92fXS8sn2lNzpwd.png)

##### 2.4.0.2 Data Source

Data Source mainly displays information about data sources, and provides operations such as adding, editing, deleting, and changing status.
![](./images/img_boxcngfo5jU8uywnDXUzZIzFOSh.png)

![](./images/img_boxcn5TTO5ze7HU1VhrpZ0CuLCb.png)

Enter the required items according to the requirements and save to create a new data source.
![](./images/img_boxcnb0aSWWklFxHhvw0UDnKdvh.png)

![](./images/img_boxcnylExq9xMAD3UR4inn97DPf.png)

When the data status is failed, finished, or stopped on the list page, moving the mouse over the status will display a detailed reason, and there are options to start/stop the operation for each status.
![](./images/img_boxcn1duZHrdqBWiXA5ZTgKykiA.png)

![](./images/img_boxcnch7XRJUpoq7uul1JQYneEh.png)

![](./images/img_boxcnWrZIELH0QJv8XD0lLuVSjc.png)

The edit operation will display the details of the most recent operation, and the required items can be edited according to the needs.
e.g.
![](./images/img_boxcn3gqhCAWWZhLCOenDnB69Yf.png)

![](./images/img_boxcnKUCUM2GdxLNkb5ouj9FhNf.png)

![](./images/img_boxcnXfye72EMKA63wllBrkAw0g.png)

### 2.5 Stream

You can add, view or delete streams here.
Streams can be created in two ways: through a wizard or SQL.
![](./images/img_boxcnrFPoBeCKJfpC7esa5LVjpf.png)

![](./images/img_boxcnMXMhWBmJ8agXiOnLFzI53f.png)

![](./images/img_boxcnMdDisB4TiaKGPPO8nG4W3g.png)

Delete the stream.
![](./images/img_boxcnQTJyBsDBdgsoxUZS9iIsyh.png)


### 2.6 Topics

You can add, view or delete topics here.
Topics can be created in two ways: through a wizard or SQL.
![](./images/img_boxcnCBzzhQtlHd0LFZTD5ZgCKc.png)

![](./images/img_boxcnESYTBBmFip8wOY73AJ3dve.png)

![](./images/img_boxcnYt2GLexUioRoQunxcZIjEf.png)

Delete the topic.
![](./images/img_boxcnt71878voVFE13icqkcYVhd.png)

You can view the consumers while subscriptions are running.
![](./images/img_boxcnLzm7nTTGJwqDAITA0bbqyg.png)

### 2.7 Admin

Currently you can only manage users. UDF and others are not available.
The "user" module is used to display user information. In this module, passwords for users other than "root" can be modified.

##### 2.7.0.1 Users

Add a user.
![](./images/img_boxcnTv40q9m5kFGILdWn8n9Ovg.png)


Delete the user.
![](./images/img_boxcnbshVKwsjSD6iU7PMux0PRg.png)

##### 2.7.0.2 Backup

The "backup" module can backup data to a specified directory.

In this module, operations such as deleting and editing users, changing statuses, etc. can also be performed.
![](./images/img_boxcnXEDguTQBPmonzMebHoML2f.png)

![](./images/img_boxcn2mloCj7ANsjuiE39FmuZxf.png)

![](./images/img_boxcnGrZHF2QD6Q16fBtnGkoLgg.png)

#### 2.7.1 Replication

The Replication module synchronizes databases.
![](./images/img_boxcnQgkwfaLFdYUDuctUKHx69f.png)

![](./images/img_boxcnKvddT2URc67qSleCqv0Zec.png)


### 2.8 Cluster

Add or drop a dnode, mnode or qnode with UI.
The Cluster module mainly performs the creation and deletion operations of DNodes, MNodes, and QNodes, and displays the relationship between the three.
Add a DNode.
![](./images/img_boxcnpgRcZkGZawjm2C5sFUpBsb.png)

Delete the DNode.
![](./images/img_boxcnEVAsGJaT6xJaL4RIQIIbNo.png)

The addition of MNodes and QNodes depends on DNodes. The addition of the two is the same. Here only the example of adding MNodes is given.
Add an MNode.
![](./images/img_boxcnlcBN0ZuLxSUwIieGD6cz2d.png)

Delete the MNode.
![](./images/img_boxcn2hKO6O0sBFJH2w7PJBESde.png)

### 2.9 Settings

- `Theme`: you can change the theme as you preferred on client side.
- Currently, this module only supports configuring dashboard jump links.

### 10.Data Out

![](./images/img_boxcnwHj5aFK671QMqyf2XtDwDg.png)

### 11.Profile

The profile module allows users to modify their passwords for the currently logged-in account.
![](./images/img_boxcnZdAocQIXm5FxYx1tIRaazl.png)

![](./images/img_boxcnRVvZuOsHGjCHx6xfJHw85g.png)

### 12.Health Report

The basic positioning of the health report is to provide an analysis of TDengine's basic operation status based on the results of monitoring from the taoskeeper monitoring library over a certain period of time.
This issue's health report module includes several sections for display (with the possibility of expansion in the future), as shown below：

##### 1.Basic information about the cluster: cluster ID, uptime, version, and expire time. This reflects the current running status

![](./images/img_boxcnoURJrQlLNMPWKMpGebSnbb.png)

##### 2.Resource usage of each DNode node.

The default health report covers the past 7 days, with options available for 1/7/30 days
![](./images/img_boxcnHg2xmjysp18s8WRpAAtiUb.png)

![](./images/img_boxcnxJVF9pIA5zmigVD1N7rcdb.png)

![](./images/img_boxcnFkd58jr4MJsGFW1tLwcfrb.png)

##### 3.Health status.

![](./images/img_boxcnzPJk3Q1bBGKlduqjBBUxVf.png)

Finally, the overall presentation of the health report is as follows.
![](./images/img_boxcnAhtCQdj5wV3pekL0yzUVxe.png)

## 3. OEM

Explorer depends on CMake options `CUS_NAME` and `CUS_PROMPT` to specify a collection of explorer assets, and build an OEM package.
Here is a prebuilt OEM assets template:
```json
{
  "logo": "://cloud.tdengine.com/static/img/site-logo.3ec8602a.webp",
  "externalLinks": [
    {
      "name": "Products",
      "url": "https://tdengine.com/products"
    },
    {
      "name": "Docs",
      "url": "https://docs.tdengine.com/"
    },
    {
      "name": "Blog",
      "url": "https://tdengine.com/blog"
    }
  ],
  "welcome": {
    "title": "TDengine Explorer",
    "subTitle": "Serverless, fully managed cloud service for TDengine",
    "mainContent": [
      "Simplified time-series data management, dramatically reducing the tools and cost needed to start, operate, and manage your time-series database at scale with built-in caching, data subscription, and stream processing. As a managed service, TDengine Cloud saves you time by taking care of clustering, backup, multi-cloud replication, and data retention on its own.",
      "TDengine is an open-source, cloud-native time-series database optimized for the Internet of Things (IoT), Connected Cars, and Industrial IoT. It enables efficient, real-time data ingestion, processing, and monitoring of TB and even PB scale data per day, generated by billions of sensors and data collectors. This document is the TDengine user manual. It introduces the basic, as well as novel concepts, in TDengine, and also talks in detail about installation, features, SQL, APIs, operation, maintenance, kernel design, and other topics. It’s written mainly for architects, developers, and system administrators."
    ]
  },
  "footer": {
    "profile": "TDengine™ is an open-source, cloud-native time-series database optimized for Internet of Things (IoT), Connected Cars, and Industrial IoT. With its built-in caching, stream processing, and data subscription capabilities, TDengine offers a simplified solution for time-series data processing.",
    "contracts": [
      {
        "icon": "",
        "url": "https://www.youtube.com/channel/UCmp-1U6GS_3V3hjir6Uq5DQ"
      },
      {
        "icon": "",
        "url": "https://twitter.com/TDengineDB"
      }
    ],
    "copyRight": "© 2023 TDengine",
    "policies": [
      {
        "name": "Careers",
        "url": "https://tdengine.com/careers"
      },
      {
        "name": "Terms",
        "url": "https://tdengine.com/terms-of-service"
      },
      {
        "name": "Privacy",
        "url": "https://tdengine.com/privacy"
      },
      {
        "name": "About",
        "url": "https://tdengine.com/about"
      }
    ]
  }
}

```

Put the prebuilt OEM settings of explorer as name `/enterprise/packaging/oem/explorer/${CUS_NAME}.json`. Compiler will help to build an OEM specified explorer package. The service name will also be renamed to `${CUS_PROMPT}-explorer`.

### 3.1 OEM is on the front-end.

To set OEM, CUS_NAME needs to be specified. If CUS_NAME is empty or equal to TDengine, it will be considered as non-OEM. Otherwise, it's OEM.
Non-OEM will display the complete login page information and complete routing and page information.
Next, we will focus on the situation with non-OEM.
1.Non-OEM will directly affect the login page, and the Git star will no longer be displayed on the login page.
![](./images/img_boxcnD0U9RbIqqid3bvsu2CHscd.png)

2.Currently, OEM accounts only display the Data In, Explorer, Visualize, Stream, Topics, Admin and Cluster routing modules. Among them, the Data In module only has CSV functionality.
![](./images/img_boxcnA3Yta86R75y0VH0JgMR11W.png)
