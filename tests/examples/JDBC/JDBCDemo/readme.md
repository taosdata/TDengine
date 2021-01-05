# How to Run the JDBC Demo Code On Linux OS
TDengine's JDBC demo project is organized in a Maven way so that users can easily compile, package and run the project. If you don't have Maven on your server, you may install it using
```
sudo apt-get install maven
```


## Install TDengine Client
Make sure you have already installed a tdengine client on your current develop environment.
Download the tdengine package on our website: ``https://www.taosdata.com/cn/all-downloads/`` and install the client.

## Run jdbcDemo using mvn plugin
run command:
```
mvn clean compile exec:java -Dexec.mainClass="com.taosdata.example.JdbcDemo"
```

and run with your customed args
```
mvn clean compile exec:java -Dexec.mainClass="com.taosdata.example.JdbcDemo" -Dexec.args="-host [HOSTNAME]"
```

## Compile the Demo Code and Run It
To compile the demo project, go to the source directory ``TDengine/tests/examples/JDBC/JDBCDemo`` and execute
```
cd TDengine/src/connector/jdbc

mvn clean package -Dmaven.test.skip=true

cp target/taos-jdbcdriver-2.0.x-dist.jar ../../../tests/examples/JDBC/JDBCDemo/target/

cd ../../../tests/examples/JDBC/JDBCDemo

mvn clean package assembly:single
```

To run it, go to ``TDengine/tests/examples/JDBC/JDBCDemo/target`` and execute
```
java -Djava.ext.dirs=.:$JAVA_HOME/jre/lib/ext -jar JDBCDemo-SNAPSHOT-jar-with-dependencies.jar -host localhost
```

