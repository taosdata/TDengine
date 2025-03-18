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
```
mvn clean package -Dmaven.test.skip=true
```

To run JDBCDemo.jar, execute
```
java -jar target/JDBCDemo-SNAPSHOT-jar-with-dependencies.jar -host [HOSTNAME]
```

