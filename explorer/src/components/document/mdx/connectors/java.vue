<template>
  <div>
    <h2 id="add-dependency">{{ $t("docs.connector.java.step1") }}</h2>
    <el-tabs model-value="maven">
      <el-tab-pane name="maven" label="Maven">
        <pre
v-highlight="
`&lt;dependency&gt;
  &lt;groupId&gt;com.taosdata.jdbc&lt;/groupId&gt;
  &lt;artifactId&gt;taos-jdbcdriver&lt;/artifactId&gt;
  &lt;version&gt;3.2.7&lt;/version&gt;
&lt;/dependency&gt;`
        "><code class="language-xml">
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="gradel" label="Gradle">
        <pre v-highlight><code class="language-groovy">dependencies {
  implementation &#39;com.taosdata.jdbc:taos-jdbcdriver:3.2.7&#39;
}
</code></pre>
      </el-tab-pane>
      <el-tab-pane
        name="spring"
        label="Spring"
      >
        <p>{{ $t('docs.connector.java.step3depdesc') }}</p>
        <pre
v-highlight="
`&lt;dependency&gt;
  &lt;groupId&gt;org.springframework.boot&lt;/groupId&gt;
  &lt;artifactId&gt;spring-boot-starter&lt;/artifactId&gt;
&lt;/dependency&gt;
&lt;dependency&gt;
  &lt;groupId&gt;org.springframework.boot&lt;/groupId&gt;
  &lt;artifactId&gt;spring-boot-starter-web&lt;/artifactId&gt;
&lt;/dependency&gt;
&lt;!--mybatis--&gt;
&lt;dependency&gt;
  &lt;groupId&gt;org.mybatis.spring.boot&lt;/groupId&gt;
  &lt;artifactId&gt;mybatis-spring-boot-starter&lt;/artifactId&gt;
  &lt;version&gt;2.2.2&lt;/version&gt;
&lt;/dependency&gt;
&lt;!--connection pool--&gt;
&lt;dependency&gt;
  &lt;groupId&gt;com.alibaba&lt;/groupId&gt;
  &lt;artifactId&gt;druid-spring-boot-starter&lt;/artifactId&gt;
  &lt;version&gt;1.2.8&lt;/version&gt;
&lt;/dependency&gt;
&lt;dependency&gt;
  &lt;groupId&gt;com.taosdata.jdbc&lt;/groupId&gt;
  &lt;artifactId&gt;taos-jdbcdriver&lt;/artifactId&gt;
  &lt;version&gt;3.2.7&lt;/version&gt;
&lt;/dependency&gt;`
        "><code class="language-xml"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="config">{{ $t("docs.connector.java.step2") }}</h2>
    <p>{{ $t("docs.docConfig.content", [" JDBC URL "]) }}</p>
    <p>
           <el-icon color="gold" :size="20">
        <Opportunity/>
      </el-icon>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <el-tabs model-value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_JDBC_URL=&quot;${jdbcURL}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
      <el-tab-pane
        name="spring"
        label="Spring"
      >
        <p>{{ $t('docs.connector.java.step3confdesc') }}</p>
        <pre 
          v-highlight="
`server:
  port: 8080

spring:
  datasource:
    driver-class-name: com.taosdata.jdbc.rs.RestfulDriver
    url: ${jdbcURL}
# using connection pools
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000
      time-between-eviction-runs-millis: 60000
      min-evictable-idle-time-millis: 300000
      validation-query: SELECT 1
      pool-prepared-statements: true
      max-pool-prepared-statement-per-connection-size: 20
# mybatis
mybatis:
  mapper-locations: classpath:mapper/*.xml`
          "><code class="language-yml"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <p>{{ $t("docs.docConfig.bottom") }}</p>
    <h2 id="connect">{{ $t("docs.connector.java.step3") }}</h2>
    <el-tabs model-value="java">
      <el-tab-pane
        name="java"
        label="Java"
      >
        <p>{{ $t('docs.connector.java.step3desc') }}</p>
        <pre
v-highlight="
`import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv(&quot;TDENGINE_JDBC_URL&quot;);
        System.out.println(jdbcUrl);
        try(Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try(Statement stmt = conn.createStatement()) {
                stmt.executeQuery(&quot;select server_version()&quot;);
            }
        }
    }
}`
        "><code class="language-java">

        </code></pre>
      </el-tab-pane>
      <el-tab-pane
        name="spring"
        label="Spring"
      >
        <ol class="seeq-ol">
          <li class="seeq-span">
            {{ $t('docs.connector.java.step3mybatisdesc1') }}
            <pre
v-highlight='
`@Select("select * from meters limit 10")
List&lt;Meter&gt; find();

int create(@Param("meter")Meter meter, @Param("tableName")String tableName);

int save(@Param("meter")Meter meter, @Param("tableName")String tableName);

Meter lastRow(@Param("tableName")String tableName);`
            '><code class="language-java">
        </code></pre>
          </li>
          <li class="seeq-span"
            >{{ $t('docs.connector.java.step3mybatisdesc2') }}
            <pre
v-highlight='
`&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
  "http://mybatis.org/dtd/mybatis-3-mapper.dtd"&gt;
&lt;mapper namespace="com.taos.example.dao.MeterMapper"&gt;
  &lt;resultMap id="Meter" type="com.taos.example.dao.Meter"&gt;
    &lt;result column="ts" property="ts"/&gt;
    &lt;result column="current" property="current"/&gt;
    &lt;result column="voltage" property="voltage"/&gt;
    &lt;result column="phase" property="phase"/&gt;
    &lt;result column="groupId" property="groupid"/&gt;
    &lt;result column="location" property="location"/&gt;
  &lt;/resultMap&gt;

  &lt;insert id="create"&gt;
    CREATE TABLE IF NOT EXISTS #{tableName, jdbcType=VARCHAR} USING meters TAGS
    (#{meter.groupId, jdbcType=INTEGER}, #{meter.location, jdbcType=VARCHAR})
  &lt;/insert&gt;

  &lt;insert id="save"&gt;
    INSERT INTO #{tableName} VALUES (#{meter.ts}, #{meter.current}, #{meter.voltage}, #{meter.phase});
  &lt;/insert&gt;

  &lt;select id="lastRow" resultMap="Meter"&gt;
    SELECT last_row(*) FROM meters;
  &lt;/select&gt;
&lt;/mapper&gt;`
            '><code class="language-xml">
        </code></pre>
          </li>
          <li class="seeq-span">
            {{ $t('docs.connector.java.step3href') }}
            <a :href="`https://github.com/taosdata/TDengine/tree/docs-cloud/docs/examples/java/spring`">
              {{ `TDengine-examples-java` }}
            </a>
          </li>
        </ol>
      </el-tab-pane>
    </el-tabs>
    <p>
      {{ $t("docs.connector.bottom1") }} {{ $t("docs.connector.bottom2") }}
      <a :href="`${$t('urlPart')}/${insertApi}`">{{
        `${$t('docs.connector.bottom2_1')}`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`${$t('urlPart')}/${selectApi}`">{{
        `${$t('docs.connector.bottom2_2')}`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`${$t('urlPart')}/${restApi}`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { isEn } from '@/const';
import { DocsProps } from '../utils'

const props = defineProps<DocsProps>()
 
const jdbcURL = computed(() => {
  return (
    "jdbc:TAOS-RS://" +
    props.url.replace(/https?:\/\//, "") +
    "?useSSL=" +
    props.url.startsWith("https") +
    "&user=" + props.user +
    "&password=" + props.password
  );
})
const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
const insertApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#insert-data' : 'develop/sql/#插入数据');
const selectApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#query-data' : 'develop/sql/#查询数据');
</script>
