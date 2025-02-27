<template>
  <div>
    <p>{{ t('connector.rest.desc') }}</p>
    <doc-config
      :url-des="t('docsConfig.url')"
      :url="instance.gatewayUrl"
      :need-token="project.isCloud"
      :token="instance.token"
    ></doc-config>

    <h2 id="insert">{{ t('connector.rest.step2') }}</h2>
    <p>{{ t('connector.rest.step2desc') }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31)&quot; \
  $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
`
      "
    ><code class="language-bash"></code></pre>

    <h2 id="query">{{ t('connector.rest.step3') }}</h2>
    <p>{{ t('connector.rest.step3desc') }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;select name, ntables, status from information_schema.ins_databases;&quot; \
  $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { docs, instance, project } from 'config';
</script>
