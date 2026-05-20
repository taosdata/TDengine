## TDengine SpringBoot + Mybatis Demo

## Need to create a test database in advance

```
$ taos -s 'create database if not exists test'

$ curl http://localhost:8080/weather/init
```

### Configure application.properties
```properties
# datasource config
spring.datasource.driver-class-name=com.taosdata.jdbc.TSDBDriver
spring.datasource.url=jdbc:TAOS://127.0.0.1:6030/test
spring.datasource.username=root
spring.datasource.password=taosdata

spring.datasource.druid.initial-size=5
spring.datasource.druid.min-idle=5
spring.datasource.druid.max-active=5
# max wait time for get connection, ms
spring.datasource.druid.max-wait=60000

spring.datasource.druid.validation-query=select SERVER_VERSION();
spring.datasource.druid.validation-query-timeout=5000
spring.datasource.druid.test-on-borrow=false
spring.datasource.druid.test-on-return=false
spring.datasource.druid.test-while-idle=true
spring.datasource.druid.time-between-eviction-runs-millis=60000
spring.datasource.druid.min-evictable-idle-time-millis=600000
spring.datasource.druid.max-evictable-idle-time-millis=900000

# mybatis
mybatis.mapper-locations=classpath:mapper/*.xml

# log 
logging.level.com.taosdata.jdbc.springbootdemo.dao=debug
```

### Main functions

* Create databases and tables
```xml
<!-- weatherMapper.xml -->
 <update id="createDB" >
        create database if not exists test;
    </update>

    <update id="createTable" >
        create table if not exists test.weather(ts timestamp, temperature int, humidity float);
    </update>
```

* Insert a single record
```xml
<!-- weatherMapper.xml -->
    <insert id="insert" parameterType="Weather" >
        insert into test.weather (ts, temperature, humidity) values (now, #{temperature,jdbcType=INTEGER}, #{humidity,jdbcType=FLOAT})
    </insert>
```
* Insert multiple records
```xml
<!-- weatherMapper.xml -->
<insert id="batchInsert" parameterType="java.util.List" >
    insert into test.weather (ts, temperature, humidity) values
    <foreach separator=" " collection="list" item="weather" index="index" >
        (now + #{index}a, #{weather.temperature}, #{weather.humidity})
    </foreach>
</insert>
```
* Pagination query
```xml
<!-- weatherMapper.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">

<mapper namespace="WeatherMapper">

    <resultMap id="BaseResultMap" type="Weather">
        <id column="ts" jdbcType="TIMESTAMP" property="ts" />
        <result column="temperature" jdbcType="INTEGER" property="temperature" />
        <result column="humidity" jdbcType="FLOAT" property="humidity" />
    </resultMap>

    <sql id="Base_Column_List">
        ts, temperature, humidity
    </sql>

    <select id="select" resultMap="BaseResultMap">
        select
        <include refid="Base_Column_List" />
        from test.weather
        order by ts desc
        <if test="limit != null">
            limit #{limit,jdbcType=BIGINT}
        </if>
        <if test="offset != null">
            offset #{offset,jdbcType=BIGINT}
        </if>
    </select>
</mapper>
```

