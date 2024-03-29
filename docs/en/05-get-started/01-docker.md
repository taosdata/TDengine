---
title: Quick Install on Docker
sidebar_label: Docker
description: This document describes how to install TDengine in a Docker container and perform queries and inserts.
---

This document describes how to install TDengine in a Docker container and perform queries and inserts.

- The easiest way to explore TDengine is through [TDengine Cloud](https://cloud.tdengine.com).
- To get started with TDengine in a non-containerized environment, see [Quick Install from Package](../../get-started/package).
- If you want to view the source code, build TDengine yourself, or contribute to the project, see the [TDengine GitHub repository](https://github.com/taosdata/TDengine).

## Run TDengine

If Docker is already installed on your computer, pull the latest TDengine Docker container image:

```shell
docker pull tdengine/tdengine:latest
```

Or the container image of specific version:

```shell
docker pull tdengine/tdengine:3.0.1.4
```

And then run the following command:

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

Note that TDengine Server 3.0 uses TCP port 6030. Port 6041 is used by taosAdapter for the REST API service. Ports 6043 through 6049 are used by taosAdapter for other connections. You can open these ports as needed.

If you need to persist data to a specific directory on your local machine, please run the following command:
```shell
docker run -d -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```
:::note

- /var/lib/taos: TDengine's default data file directory. The location can be changed via [configuration file]. Also you can modify ~/data/taos/dnode/data to your any local empty data directory
- /var/log/taos: TDengine's default log file directory. The location can be changed via [configure file]. you can modify ~/data/taos/dnode/log to your any local empty log directory

:::


Run the following command to ensure that your container is running:

```shell
docker ps
```

Enter the container and open the `bash` shell:

```shell
docker exec -it <container name> bash
```

You can now access TDengine or run other Linux commands.

Note: For information about installing docker, see the [official documentation](https://docs.docker.com/get-docker/).

## Open the TDengine CLI

On the container, run the following command to open the TDengine CLI:

```
$ taos

taos>

```

## Test data insert performance

After your TDengine Server is running normally, you can run the taosBenchmark utility to test its performance:

Start TDengine service and execute `taosBenchmark` (formerly named `taosdemo`) in a terminal.

```bash
taosBenchmark
```

This command creates the `meters` supertable in the `test` database. In the `meters` supertable, it then creates 10,000 subtables named `d0` to `d9999`. Each table has 10,000 rows and each row has four columns: `ts`, `current`, `voltage`, and `phase`. The timestamps of the data in these columns range from 2017-07-14 10:40:00 000 to 2017-07-14 10:40:09 999. Each table is randomly assigned a `groupId` tag from 1 to 10 and a `location` tag of either `California.Campbell`, `California.Cupertino`, `California.LosAngeles`, `California.MountainView`, `California.PaloAlto`, `California.SanDiego`, `California.SanFrancisco`, `California.SanJose`, `California.SantaClara` or `California.Sunnyvale`.

The `taosBenchmark` command creates a deployment with 100 million data points that you can use for testing purposes. The time required to create the deployment depends on your hardware. On most modern servers, the deployment is created in ten to twenty seconds.

You can customize the test deployment that taosBenchmark creates by specifying command-line parameters. For information about command-line parameters, run the `taosBenchmark --help` command. For more information about taosBenchmark, see [taosBenchmark](../../reference/taosbenchmark).

## Test data query performance

After using `taosBenchmark` to create your test deployment, you can run queries in the TDengine CLI to test its performance:

From the TDengine CLI (taos) query the number of rows in the `meters` supertable:

```sql
SELECT COUNT(*) FROM test.meters;
```

Query the average, maximum, and minimum values of all 100 million rows of data:

```sql
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

Query the number of rows whose `location` tag is `California.SanFrancisco`:

```sql
SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
```

Query the average, maximum, and minimum values of all rows whose `groupId` tag is `10`:

```sql
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
```

Query the average, maximum, and minimum values for table `d10` in 10 second intervals:

```sql
SELECT FIRST(ts), AVG(current), MAX(voltage), MIN(phase) FROM test.d10 INTERVAL(10s);
```

In the query above you are selecting the first timestamp (ts) in the interval, another way of selecting this would be `\_wstart` which will give the start of the time window. For more information about windowed queries, see [Time-Series Extensions](../../taos-sql/distinguished/).

## Additional Information

For more information about deploying TDengine in a Docker environment, see [Deploying TDengine with Docker](../../deployment/docker).
