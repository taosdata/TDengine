# How to Run the JDBC Demo Code On A Linux OS
TDengine's JDBC demo project is organized in a Maven way so that users can easily compile, package and run the project. If you don't have Maven on your server, you may install it using
<pre>sudo apt-get install maven</pre>

## Compile and Install JDBC Driver
TDengine's JDBC driver jar is not yet published to maven center repo, so we need to manually compile it and install it to the local Maven repository. This can be easily done with Maven. Go to source directory of the JDBC driver ``TDengine/src/connector/jdbc`` and execute
<pre>mvn clean package install</pre>

## Compile the Demo Code and Run It
To compile the demo project, go to the source directory ``TDengine/tests/examples/JDBC/JDBCDemo`` and execute
<pre>mvn clean assembly:single package</pre>
The ``pom.xml`` is configured to package all the dependencies into one executable jar file. To run it, go to ``TDengine/tests/examples/JDBC/JDBCDemo/target`` and execute
<pre>java -jar jdbcdemo-1.0-SNAPSHOT-jar-with-dependencies.jar</pre>
