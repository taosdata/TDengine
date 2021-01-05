# How to Run the JDBC Demo Code On Linux OS
TDengine's JDBC demo project is organized in a Maven way so that users can easily compile, package and run the project. If you don't have Maven on your server, you may install it using
<pre>sudo apt-get install maven</pre>

## Install TDengine Client
Make sure you have already installed a tdengine client on your current develop environment.
Download the tdengine package on our website: ``https://www.taosdata.com/cn/all-downloads/`` and install the client.

## Run jdbcDemo using mvn plugin
run command:
<pre> mvn clean compile exec:java -Dexec.mainClass="com.taosdata.example.JdbcDemo"</pre>
and run with your customed args
<pre>mvn clean compile exec:java -Dexec.mainClass="com.taosdata.example.JdbcDemo" -Dexec.args="-host [HOSTNAME]"</pre>

## Compile the Demo Code and Run It

To compile the demo project, go to the source directory ``TDengine/tests/examples/JDBC/JDBCDemo`` and execute

<pre>
mvn clean package assembly:single
</pre>

The ``pom.xml`` is configured to package all the dependencies into one executable jar file.

To run it, go to ``examples/JDBC/JDBCDemo/target`` and execute
<pre>java -jar jdbcChecker-SNAPSHOT-jar-with-dependencies.jar -host localhost</pre>
