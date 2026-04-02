<template>
  <div>
    <h2 id="create-project">{{ $t('docs.connector.r.step1') }}</h2>
    <p>{{ $t('docs.connector.r.step11desc') }}</p>
    <p>{{ $t('docs.connector.r.step12desc') }}</p>
    <pre v-highlight><code class="language-r">install.packages("RJDBC", repos='http://cran.us.r-project.org')
</code></pre>
    <p
      >{{ $t('docs.connector.r.step13desc') }}<a href="https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver"> {{ $t('docs.connector.r.step13desc1') }}</a>
      {{ $t('docs.connector.r.step13desc2') }}</p
    >
    <pre v-highlight><code class="language-r">[path]/taos-jdbcdriver-X.X.X-dist.jar
</code></pre>
    <doc-config
      :need-token="false"
      :url="jdbcURL"
      :url-key="'TDENGINE_JDBC_URL'"
      :url-des="'JDBC URL '"
    ></doc-config>
    <p>{{ $t('docs.connector.r.step21desc') }}</p>
    <pre v-highlight><code class="language-r">library(DBI)
library(rJava)
library(RJDBC)
</code></pre>
    <p>{{ $t('docs.connector.r.step22desc') }}</p>
    <pre v-highlight><code class="language-r">driverPath &lt;- "[path]/taos-jdbcdriver-X.X.X-dist.jar"

url &lt;- Sys.getenv("TDENGINE_JDBC_URL")
</code></pre>
    <p>{{ $t('docs.connector.r.step23desc') }}</p>
    <h2 id="connect">{{ $t('docs.connector.r.step3') }}</h2>
    <p>{{ $t('docs.connector.r.step31desc') }}</p>
    <pre v-highlight><code class="language-r">drv &lt;- JDBC("com.taosdata.jdbc.rs.RestfulDriver", driverPath)
</code></pre>
    <p>{{ $t('docs.connector.r.step32desc') }}</p>
    <pre v-highlight><code class="language-r">conn &lt;- dbConnect(drv, url)
</code></pre>
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
import DocConfig from '@/components/document/commonConfig.vue';
import { DocsProps } from '../utils'
import { isEn } from '@/const';

const props = defineProps<DocsProps>()

const jdbcURL = computed(() => {
  return 'jdbc:TAOS-RS://' + props.url.replace(/https?:\/\//, '') + '?useSSL=' + props.url.startsWith('https') + '&token=' + props.token;
})

const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
const insertApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#insert-data' : 'develop/sql/#插入数据');
const selectApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#query-data' : 'develop/sql/#查询数据');

</script>
