---
sidebar_label: Docker
title: Quick Install on Docker
---

This document describes how to install TDengine in a Docker container and perform queries and inserts. To get started with TDengine in a non-containerized environment, see [Quick Install](../../get-started/package). If you want to view the source code, build TDengine yourself, or contribute to the project, see the [TDengine GitHub repository](https://github.com/taosdata/TDengine).

## Run TDengine

If Docker is already installed on your computer, run the following command:

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

Note that TDengine Server uses TCP port 6030. Port 6041 is used by taosAdapter for the REST API service. Ports 6043 through 6049 are used by taosAdapter for other connectors. You can open these ports as needed.

Run the following command to ensure that your container is running:

```shell
docker ps
```

Enter the container and open the bash shell:

```shell
docker exec -it <container name> bash
```

You can now access TDengine or run other Linux commands.

Note: For information about installing docker, see the [official documentation](https://docs.docker.com/get-docker/).

## Insert Data into TDengine

You can use the `taosBenchmark` tool included with TDengine to write test data into your deployment.

To do so, run the following command:

   ```bash
   $ taosBenchmark
   
   ```

This command creates the `meters` supertable in the `test` database. In the `meters` supertable, it then creates 10,000 subtables named `d0` to `d9999`. Each table has 10,000 rows and each row has four columns: `ts`, `current`, `voltage`, and `phase`. The timestamps of the data in these columns range from 2017-07-14 10:40:00 000 to 2017-07-14 10:40:09 999. Each table is randomly assigned a `groupId` tag from 1 to 10 and a `location` tag of either `Campbell`, `Cupertino`, `Los Angeles`, `Mountain View`, `Palo Alto`, `San Diego`, `San Francisco`, `San Jose`, `Santa Clara` or `Sunnyvale`.

   The `taosBenchmark` command creates a deployment with 100 million data points that you can use for testing purposes. The time required depends on the hardware specifications of the local system.

   You can customize the test deployment that taosBenchmark creates by specifying command-line parameters. For information about command-line parameters, run the `taosBenchmark --help` command. For more information about taosBenchmark, see [taosBenchmark](/reference/taosbenchmark).

## Open the TDengine CLI

On the container, run the following command to open the TDengine CLI: 

```
$ taos

taos> 

```

## Query Data in TDengine

After using taosBenchmark to create your test deployment, you can run queries in the TDengine CLI to test its performance. For example:

From the TDengine CLI query the number of rows in the `meters` supertable:

```sql
select count(*) from test.meters;
```

Query the average, maximum, and minimum values of all 100 million rows of data:

```sql
select avg(current), max(voltage), min(phase) from test.meters;
```

Query the number of rows whose `location` tag is `San Francisco`:

```sql
select count(*) from test.meters where location="San Francisco";
```

Query the average, maximum, and minimum values of all rows whose `groupId` tag is `10`:

```sql
select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

Query the average, maximum, and minimum values for table `d10` in 1 second intervals:

```sql
select first(ts), avg(current), max(voltage), min(phase) from test.d10 interval(1s);
```
In the query above you are selecting the first timestamp (ts) in the interval, another way of selecting this would be _wstart which will give the start of the time window. For more information about windowed queries, see [Time-Series Extensions](../../taos-sql/distinguished/).

## Additional Information

For more information about deploying TDengine in a Docker environment, see [Using TDengine in Docker](../../reference/docker).
