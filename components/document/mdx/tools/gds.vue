<template>
  <div>
    <p>
      {{ t('tools.gds.topdesc') }}
      <a href="https://datastudio.google.com/data?search=TDengine">{{ t('tools.gds.topconnector') }}</a
      >{{ t('tools.gds.topdesc1') }}
    </p>
    <p>
      {{ t('tools.gds.topdesc2') }}
      <a href="https://github.com/taosdata/gds-connector/blob/master/README.md">GitHub</a>
      {{ t('tools.gds.topdesc3') }}
    </p>
    <h2 id="choose-data-source">{{ t('tools.gds.step1') }}</h2>
    <p>
      {{ t('tools.gds.step1desc')
      }}<a href="https://datastudio.google.com/data?search=TDengine">{{ t('tools.gds.step1desc1') }}</a>
      {{ t('tools.gds.step1desc2') }}
    </p>
    <p>
      <img src="../assets/gds/gds_data_source.webp" alt="Data Studio Data Source Selection" />
    </p>
    <h2 id="connector-configuration">{{ t('tools.gds.step2') }}</h2>
    <h3 id="mandatory-config">{{ t('tools.gds.step21') }}</h3>
    <h4 id="url">{{ t('tools.gds.step21desc') }}</h4>
    <pre v-highlight="`${instance.gatewayUrl}`"><code class="language-bash"></code></pre>
    <h4 id="tdengine-cloud-token">{{ t('tools.gds.step211') }}</h4>
    <pre v-highlight="`${instance.token}`"><code class="language-bash"></code></pre>
    <h4 id="database">{{ t('tools.gds.step212') }}</h4>
    <p>{{ t('tools.gds.step212desc') }}</p>
    <h4 id="table">{{ t('tools.gds.step213') }}</h4>
    <p>{{ t('tools.gds.step213desc') }}</p>
    <p>
      <strong>{{ t('tools.gds.step213desc1') }}</strong
      >{{ t('tools.gds.step213desc2') }}
    </p>
    <h3 id="optional-config">{{ t('tools.gds.step22') }}</h3>
    <h4 id="query-range-start-date--end-date">
      {{ t('tools.gds.step221') }}
    </h4>
    <p>{{ t('tools.gds.step221desc') }}</p>
    <pre
      v-highlight="
        `2022-05-12 18:24:15
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('tools.gds.step221desc1') }}</p>
    <p>{{ t('tools.gds.step221desc2') }}</p>
    <pre
      v-highlight
    ><code class="language-SQL">-- select * from table_name where ts &gt;= start_date and ts &lt;= end_date
select * from test.demo where ts &gt;= &#39;2022-05-10 18:24:15&#39; and ts&lt;=&#39;2022-05-12 18:24:15&#39;
</code></pre>
    <p>{{ t('tools.gds.step221desc3') }}</p>
    <p>
      <img src="../assets/gds/gds_cloud_login.webp" alt="TDengine Cloud Config Page" />
    </p>
    <p>{{ t('tools.gds.step221desc4') }}</p>
    <h2 id="create-report-or-dashboard">{{ t('tools.gds.step3') }}</h2>
    <p>{{ t('tools.gds.step3desc') }}</p>
    <p>
      {{ t('tools.gds.step3desc1')
      }}<a :href="TdDocsUrl + '/third-party/google-data-studio/'"> {{ t('tools.gds.step3desc2') }}</a>
      {{ t('tools.gds.step3desc3') }}
    </p>
  </div>
</template>
<script lang="ts" setup>
import { t } from 'locales';
import { instance, TdDocsUrl } from 'config';
</script>
