
## TDengine Spring JDBC Template Demo

`Spring JDBC Template` 简化了原生 JDBC Connection 获取释放等操作，使得操作数据库更加方便。

### 配置

修改 `src/main/resources/applicationContext.xml` 文件中 TDengine 的配置信息：

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

### 打包运行

进入 `TDengine/tests/examples/JDBC/SpringJdbcTemplate` 目录下，执行以下命令可以生成可执行 jar 包。
```shell
mvn clean package
```
打包成功之后，进入 `target/` 目录下，执行以下命令就可运行测试：
```shell
java -jar target/SpringJdbcTemplate-1.0-SNAPSHOT-jar-with-dependencies.jar
```