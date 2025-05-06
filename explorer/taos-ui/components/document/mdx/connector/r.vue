<template>
  <div>
    <h2 id="create-project">{{ t('connector.r.step1') }}</h2>
    <p>{{ t('connector.r.step11desc') }}</p>
    <p>{{ t('connector.r.step12desc') }}</p>
    <pre v-highlight><code class="language-r">install.packages("RJDBC", repos='http://cran.us.r-project.org')
</code></pre>
    <p>
      {{ t('connector.r.step13desc')
      }}<a href="https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver">
        {{ t('connector.r.step13desc1') }}</a
      >
      {{ t('connector.r.step13desc2') }}
    </p>
    <pre v-highlight><code class="language-r">[path]/taos-jdbcdriver-X.X.X-dist.jar
</code></pre>
    <doc-config :need-token="false" :url="jdbcURL" url-key="TDENGINE_JDBC_URL" :url-des="'JDBC URL '"></doc-config>
    <p>{{ t('connector.r.step21desc') }}</p>
    <pre v-highlight><code class="language-r">library(DBI)
library(rJava)
library(RJDBC)
</code></pre>
    <p>{{ t('connector.r.step22desc') }}</p>
    <pre v-highlight><code class="language-r">driverPath &lt;- "[path]/taos-jdbcdriver-X.X.X-dist.jar"

url &lt;- Sys.getenv("TDENGINE_JDBC_URL")
</code></pre>
    <p>{{ t('connector.r.step23desc') }}</p>
    <h2 id="connect">{{ t('connector.r.step3') }}</h2>
    <p>{{ t('connector.r.step31desc') }}</p>
    <pre v-highlight><code class="language-r">drv &lt;- JDBC("com.taosdata.jdbc.rs.RestfulDriver", driverPath)
</code></pre>
    <p>{{ t('connector.r.step32desc') }}</p>
    <pre v-highlight><code class="language-r">conn &lt;- dbConnect(drv, url)
</code></pre>
    <p>
      {{ t('connector.bottom2') }}
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
