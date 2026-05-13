<template>
  <div>
    <h2 id="install-connector">{{ $t('docs.connector.node.step1') }}</h2>
    <pre
      v-highlight="
        `npm install @tdengine/websocket
`
      "
    ><code class="language-bash"></code></pre>
    <doc-config :url="url" :token="token"></doc-config>
    <h2 id="connect">{{ $t('docs.connector.node.step3') }}</h2>
    <pre v-highlight><code class="language-javascript">const { options, connect } = require(&quot;@tdengine/rest&quot;);

async function test() {
  options.url = import.meta.env.TDENGINE_URL;
  options.query = { token: import.meta.env.TDENGINE_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query(&quot;show databases&quot;);
    res.toString();
  } catch (err) {
    console.log(err);
  }
}

test();
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
import { isEn } from '@/const';
import { DocsProps } from '../utils';
import DocConfig from '@/components/document/commonConfig.vue';

defineProps<DocsProps>();

const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
const insertApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#insert-data' : 'develop/sql/#插入数据');
const selectApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#query-data' : 'develop/sql/#查询数据');
</script>
