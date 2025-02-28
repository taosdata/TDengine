<template>
  <div>
    <h2 id="add-dependency">{{ t('connector.java.step1') }}</h2>
    <el-tabs model-value="maven">
      <el-tab-pane name="maven" label="Maven">
        <pre v-highlight><code class="language-xml">    &lt;dependency&gt;
      &lt;groupId&gt;com.taosdata.jdbc&lt;/groupId&gt;
      &lt;artifactId&gt;taos-jdbcdriver&lt;/artifactId&gt;
      &lt;version&gt;3.2.9&lt;/version&gt;
    &lt;/dependency&gt;
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="spring" label="Spring">
        <p>{{ t('connector.java.step3depdesc') }}</p>
        <pre v-highlight><code class="language-xml">
&lt;dependency&gt;
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
  &lt;version&gt;3.2.9&lt;/version&gt;
&lt;/dependency&gt;
        </code>
        </pre>
      </el-tab-pane>
    </el-tabs>

    <doc-config
      :need-token="false"
      :url="jdbcURL"
      url-key="TDENGINE_JDBC_URL"
      :url-des="'JDBC URL '"
      :show-spring-tab="true"
    ></doc-config>

    <h2 id="connect">{{ t('connector.java.step3') }}</h2>
    <el-tabs model-value="java">
      <el-tab-pane name="java" label="Java">
        <p>{{ t('connector.java.step3desc') }}</p>
        <pre v-highlight><code class="language-java">
import java.sql.Connection;
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
}
        </code>
        </pre>
      </el-tab-pane>
      <el-tab-pane name="spring" label="Spring">
        <ol class="seeq-ol">
          <li class="seeq-span">
            {{ t('connector.java.step3mybatisdesc1') }}
            <pre v-highlight><code class="language-java">
  @Select("select * from meters limit 10")
  List&lt;/Meter&gt;find();

  int create(@Param("meter")Meter meter, @Param("tableName")String tableName);

  int save(@Param("meter")Meter meter, @Param("tableName")String tableName);

  Meter lastRow(@Param("tableName")String tableName);
        </code></pre>
          </li>
          <li class="seeq-span">
            {{ t('connector.java.step3mybatisdesc2') }}
            <pre v-highlight><code class="language-xml">
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
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
    INSERT INTO #{tableName} VALUES (#{meter.ts}, #{meter.current}, #{meter.voltage}, #{meter.phase}) ;
  &lt;/insert&gt;

  &lt;select id="lastRow" resultMap="Meter"&gt;
    SELECT last_row(*) FROM meters;
  &lt;/select&gt;
&lt;/mapper&gt;
        </code>
        </pre>
          </li>
          <li class="seeq-span">
            {{ t('connector.java.step3href') }}
            <a :href="`https://github.com/taosdata/TDengine/tree/docs-cloud/docs/examples/java/spring`">
              {{ `https://github.com/taosdata/TDengine/tree/docs-cloud/docs/examples/java/spring` }}
            </a>
          </li>
        </ol>
      </el-tab-pane>
    </el-tabs>

    <p>
      {{ t('connector.bottom1') }} {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/programming/insert/`">{{ `${docs.urlPrefix}/programming/insert/` }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/programming/query/`">{{ `${docs.urlPrefix}/programming/query/` }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { jdbcURL } from '../utils';
import { docs } from 'config';
</script>
<style lang="scss" scoped>
.docs p {
  line-height: 30px;
}

.seeq-ol {
  padding-left: 0;

  .seeq-span {
    padding-left: 20px;
    line-height: 30px;
    text-indent: -20px;

    .pre-code {
      text-indent: 0;

      // margin-left: -17px;
    }
  }
}
</style>
