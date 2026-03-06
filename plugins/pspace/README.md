# taosX pSpace plugin

pSpace is a time-series database. taosX provides an SDK wrapper for pSpace and supports historical data migration, real-time data synchronization, and continuous query synchronization.

## Example

1. Check connection

Run the following command:

```shell
java -jar taosx-pspace.jar -m check -c ./example/check.toml
```

Configuration file: [./example/check.toml](example/check.toml)

Output:

```JSON
{
  "valid": true,
  "support": true,
  "data_source": "pspace",
  "version": "7.1"
}
```

2. Fetch pSpace nodes

Run the following command:

```shell
java -jar taosx-pspace.jar -m nodes -c ./example/nodes.toml
```

Configuration file: [./example/nodes.toml](example/nodes.toml)

Output:

```JSON
[
  {
    "id": 150016,
    "name": "北京",
    "long_name": "\\北京",
    "is_leaf": false
  },
  {
    "id": 150028,
    "name": "上海",
    "long_name": "\\上海",
    "is_leaf": true
  }
]
```

3. Fetch pSpace tags

Run the following command:

```shell
java -jar taosx-pspace.jar -m points -c ./example/points.toml
```

Configuration file: [./example/points.toml](example/points.toml)

Output:

```JSON
[
  {
    "id": 150019,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\气温",
    "desc": ""
  },
  {
    "id": 150021,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\望京\\气温",
    "desc": ""
  },
  {
    "id": 150023,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\酒仙桥\\气温",
    "desc": ""
  }
]
```

4. Run the history migration task

Run the following command:

```shell
java -jar taosx-pspace.jar -m run -c ./example/query.toml
```

Configuration file: [./example/query.toml](example/query.toml)

5. Run the realtime subscription task

Run the following command:

```shell
java -jar taosx-pspace.jar -m run -c ./example/subscribe.toml
```

Configuration file: [./example/subscribe.toml](example/subscribe.toml)

6. Run the query sync task

Run the following command:

```shell
java -jar taosx-pspace.jar -m run -c ./example/querySync.toml
```

Configuration file: [./example/querySync.toml](example/querySync.toml)

7. Print help

Run the following command:

```shell
java -jar taosx-pspace.jar -h
```

Output

```shell
Usage: taosx-pspace [-hV] -c=<config> -m=<mode>
taosX pSpace plugin - command line tool to run pSpace tasks
  -c, --config=<config>   Path to configuration file
  -h, --help              Show this help message and exit.
  -m, --mode=<mode>       Task mode: check, nodes, points, query, subscribe, querySync
  -V, --version           Print version information and exit.
```

8. Print version

Run the following command:

```shell
java -jar target/taosx-pspace.jar -V
```

Output:

```shell
version: 1.11.0 (core-1.11.0 debug)
git: ab5a0134b0d01f0e20fcbf4d5acb1bf7fab84115
build: macos-x86_64 2026-01-23 14:47:09 +08:00
```

## Pre-build

Install the pSpace javaSDK to the local Maven repository:

```shell
mvn install:install-file \
-Dfile=./sdk/pSpace-javaSDK-2.1.10-jar-with-dependencies.jar \
-DgroupId=com.sunwayland.pspace \
-DartifactId=pSpace-javaSDK \
-Dversion=2.1.10 \
-Dpackaging=jar
```

## Build

```shell
mvn clean package
```
