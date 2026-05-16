<template>
  <div>
    <p>
      {{ $t('docs.virtual.gds.topdesc') }}
      <a href="https://datastudio.google.com/data?search=TDengine">{{ $t('docs.virtual.gds.topconnector') }}</a
      >{{ $t('docs.virtual.gds.topdesc1') }}
    </p>
    <p>
      {{ $t('docs.virtual.gds.topdesc2') }}
      <a href="https://github.com/taosdata/gds-connector/blob/master/README.md">GitHub</a>
      {{ $t('docs.virtual.gds.topdesc3') }}
    </p>
    <h2 id="choose-data-source">{{ $t('docs.virtual.gds.step1') }}</h2>
    <p>
      {{ $t('docs.virtual.gds.step1desc')
      }}<a href="https://datastudio.google.com/data?search=TDengine">{{ $t('docs.virtual.gds.step1desc1') }}</a>
      {{ $t('docs.virtual.gds.step1desc2') }}
    </p>
    <p v-if="!$IS_OEM">
      <img src="../assets/gds/gds_data_source.webp" alt="Looker Studio Data Source Selection" />
    </p>
    <h2 id="connector-configuration">{{ $t('docs.virtual.gds.step2') }}</h2>
    <h3 id="mandatory-config">{{ $t('docs.virtual.gds.step21') }}</h3>
    <h4 id="url">{{ $t('docs.virtual.gds.step21desc') }}</h4>
    <p>
      <el-icon color="gold" :size="20">
        <Opportunity />
      </el-icon>
      <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
    </p>
    <pre v-highlight="`${cloud_url}`"><code class="language-bash"></code></pre>
    <h4 id="tdengine-cloud-token">{{ $t('docs.virtual.gds.step211') }}</h4>
    <pre v-highlight="`${cloud_token}`"><code class="language-bash"></code></pre>
    <h4 id="database">{{ $t('docs.virtual.gds.step212') }}</h4>
    <p>{{ $t('docs.virtual.gds.step212desc') }}</p>
    <h4 id="table">{{ $t('docs.virtual.gds.step213') }}</h4>
    <p>{{ $t('docs.virtual.gds.step213desc') }}</p>
    <p>
      <strong>{{ $t('docs.virtual.gds.step213desc1') }}</strong
      >{{ $t('docs.virtual.gds.step213desc2') }}
    </p>
    <h3 id="optional-config">{{ $t('docs.virtual.gds.step22') }}</h3>
    <h4 id="query-range-start-date--end-date">
      {{ $t('docs.virtual.gds.step221') }}
    </h4>
    <p>{{ $t('docs.virtual.gds.step221desc') }}</p>
    <pre
      v-highlight="
        `2022-05-12 18:24:15
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ $t('docs.virtual.gds.step221desc1') }}</p>
    <p>{{ $t('docs.virtual.gds.step221desc2') }}</p>
    <pre
      v-highlight
    ><code class="language-SQL">-- select * from table_name where ts &gt;= start_date and ts &lt;= end_date
select * from test.demo where ts &gt;= &#39;2022-05-10 18:24:15&#39; and ts&lt;=&#39;2022-05-12 18:24:15&#39;
</code></pre>
    <p>{{ $t('docs.virtual.gds.step221desc3') }}</p>
    <p v-if="!$IS_OEM">
      <img src="../assets/gds/gds_cloud_login.webp" alt="TDengine  Config Page" />
    </p>
    <p>{{ $t('docs.virtual.gds.step221desc4') }}</p>
    <h2 id="create-report-or-dashboard">{{ $t('docs.virtual.gds.step3') }}</h2>
    <p>{{ $t('docs.virtual.gds.step3desc') }}</p>
    <p v-if="!$IS_OEM">
      {{ $t('docs.virtual.gds.step3desc1') }}&nbsp;<a :href="`${$t('urlPart')}/third-party/google-data-studio/`">{{
        $t('docs.virtual.gds.step3desc2')
      }}</a>
      {{ $t('docs.virtual.gds.step3desc3') }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';
const { $IS_OEM } = inject('globalCustomProperties') as GlobalCustomProperties;

const props = defineProps<DocsProps>();

const cloud_url = computed(() => {
  return props.url;
});
const cloud_token = computed(() => {
  return props.token;
});
</script>
