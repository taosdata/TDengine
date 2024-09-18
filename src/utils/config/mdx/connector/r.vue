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
      {{ $t('docs.connector.bottom2') }}
      <a :href="`${$t('urlPart')}/reference/taos-sql/insert/`">{{ $t('docs.connector.r.insertdata') }}</a>
      {{ $t('docs.connector.bottomand') }}
      <a :href="`${$t('urlPart')}/reference/taos-sql/select/`">{{ $t('docs.connector.r.querydata') }}</a
      >{{ $t('docs.connector.bottom3end') }}
    </p>
    <p>
      {{ $t('docs.connector.bottom3') }}
      <a :href="`${$t('urlPart')}${$t('docs.connector.bottom3_1')}`">REST API</a>{{ $t('docs.connector.bottom3end') }}
    </p>
  </div>
</template>

<script>
import DocConfig from '@/components/DocConfig/index.vue';
export default {
  components: { DocConfig },
  props: {
    token: {
      type: String,
      default: ''
    },
    url: {
      type: String,
      default: ''
    }
  },
  data() {
    return {};
  },
  computed: {
    jdbcURL() {
      return 'jdbc:TAOS-RS://' + this.url.replace(/https?:\/\//, '') + '?useSSL=' + this.url.startsWith('https') + '&token=' + this.token;
    },
    urlPart() {
      return this.$i18n.locale.includes('en') ? "tdengine" : "taosdata";
    },
    restapi() {
      return this.$i18n.locale.includes('en') ? "reference" : "connector";
    },
  }
};
</script>
