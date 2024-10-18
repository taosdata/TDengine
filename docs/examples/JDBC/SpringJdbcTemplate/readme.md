
## TDengine Spring JDBC Template Demo

`Spring JDBC Template` simplifies the operations of acquiring and releasing native JDBC Connections, making database operations more convenient.

### Configuration

Modify the TDengine configuration in the `src/main/resources/applicationContext.xml` file:

```xml
<bean id="dataSource" class="org.springframework.jdbc.datasource.DriverManagerDataSource">
    <property name="driverClassName" value="com.taosdata.jdbc.TSDBDriver"></property>
    <property name="url" value="jdbc:TAOS://127.0.0.1:6030/test"></property>
    <property name="username" value="root"></property>
    <property name="password" value="taosdata"></property>
</bean>

<bean id = "jdbcTemplate"  class="org.springframework.jdbc.core.JdbcTemplate" >
    <property name="dataSource" ref = "dataSource" ></property>
</bean>
```

### Package and run

Navigate to the `TDengine/tests/examples/JDBC/SpringJdbcTemplate` directory and execute the following commands to generate an executable jar file.

```shell
mvn clean package
```
After successfully packaging, navigate to the `target/` directory and execute the following commands to run the tests:

```shell
java -jar target/SpringJdbcTemplate-1.0-SNAPSHOT-jar-with-dependencies.jar
```
