<template>
  <div>
    <h2 id="install-connector">{{ t('connector.node.step1') }}</h2>
    <pre
      v-highlight="
        `npm install @tdengine/websocket
`
      "
    ><code class="language-bash"></code></pre>
    <doc-config :url="wsUrl" :token="instance.token" :need-token="project.isCloud"></doc-config>
    <h2 id="connect">{{ t('connector.node.step3') }}</h2>
    <pre v-highlight><code class="language-javascript">const taos = require(&quot;@tdengine/websocket&quot;);
const url = process.env.TDENGINE_CLOUD_URL;
const token = process.env.TDENGINE_CLOUD_TOKEN;
let conn = null;
  try {
    const conf = new taos.WSConfig(url);
    conf.setToken(token);
    conn = await taos.sqlConnect(conf);
  } catch (err) {
    throw err;
  } finally {
    if (conn) {
      await conn.close();
    }
  }
</code></pre>
    <p>
      {{ t('connector.bottom1') }} {{ t('connector.bottom2') }}
      <a target="_blank" :href="`${docs.urlPrefix}/programming/insert/`">{{
        `${docs.urlPrefix}/programming/insert/`
      }}</a>
      {{ t('connector.bottomand') }}
      <a target="_blank" :href="`${docs.urlPrefix}/programming/query/`">{{ `${docs.urlPrefix}/programming/query/` }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a target="_blank" :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a
      >{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { docs, instance, project } from 'config';

const wsUrl = computed(() => instance.gatewayUrl.replace('http', 'ws'));
</script>
