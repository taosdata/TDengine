# How to Run the Consumer Demo Code On Linux OS

TDengine's Consumer demo project is organized in a Maven way so that users can easily compile, package and run the project. If you don't have Maven on your server, you may install it using

```shell
sudo apt-get install maven
```shell

## Install TDengine Client and TaosAdapter

Make sure you have already installed a tdengine client on your current develop environment.
Download the tdengine package on our website: ``https://www.taosdata.com/cn/all-downloads/`` and install the client.

## Run Consumer Demo using mvn plugin

run command:

```shell
mvn clean compile exec:java -Dexec.mainClass="com.taosdata.ConsumerDemo"
```shell

## Custom configuration

```shell
# the host of TDengine server
export TAOS_HOST="127.0.0.1"

# the port of TDengine server
export TAOS_PORT="6041"

# the consumer type, can be "ws" or "jni"
export TAOS_TYPE="ws"

# the number of consumers
export TAOS_JDBC_CONSUMER_NUM="1"

# the number of processors to consume
export TAOS_JDBC_PROCESSOR_NUM="2"

# the number of records to be consumed per processor per second
export TAOS_JDBC_RATE_PER_PROCESSOR="1000"

# poll wait time in ms
export TAOS_JDBC_POLL_SLEEP="100"
```shell

## Run Consumer Demo using jar

To compile the demo project, go to the source directory ``TDengine/tests/examples/JDBC/consumer-demo`` and execute

```shell
mvn clean package assembly:single
```shell

To run ConsumerDemo.jar, go to ``TDengine/tests/examples/JDBC/consumer-demo`` and execute

```shell
java -jar target/ConsumerDemo-jar-with-dependencies.jar
```shell
