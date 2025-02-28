<template>
  <div>
    <p>
      {{ t('dataIn.influxdb.desc', [t('dataIn.opentsdbjson.title'), cloudText]) }}
    </p>
    <h2 id="config">{{ t('dataIn.opentsdbjson.step1') }}</h2>
    <p>
      {{ t('docsConfig.content', [' Token ' + t('connector.bottomand') + ' URL '], cloudText) }}
    </p>
    <el-tabs model-value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export ${tokenKey}=&quot;${instance.token}&quot;
export ${urlKey}=&quot;${instance.gatewayUrl}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set ${tokenKey}=&quot;${instance.token}&quot;
set ${urlKey}=&quot;${instance.gatewayUrl}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:${tokenKey}=&quot;${instance.token}&quot;
$env:${urlKey}=&quot;${instance.gatewayUrl}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="insert">{{ t('dataIn.opentsdbjson.step2') }}</h2>
    <p>{{ t('dataIn.opentsdbjson.step2desc') }}</p>
    <pre v-highlight><code class="language-text">/opentsdb/v1/put/json/&lt;db&gt;?token=&lt;cloud_token&gt;
</code></pre>
    <h2 id="examples">{{ t('dataIn.opentsdbjson.step3') }}</h2>
    <h3>{{ t('dataIn.opentsdbjson.step31') }}</h3>
    <pre
      v-highlight="
        `curl --request POST &quot;$${urlKey}/opentsdb/v1/put/json/${db_name}?token=$${tokenKey}&quot; --data-binary &quot;{\&quot;metric\&quot;:\&quot;meter_current\&quot;,\&quot;timestamp\&quot;:1646846400,\&quot;value\&quot;:10.3,\&quot;tags\&quot;:{\&quot;groupid\&quot;:2,\&quot;location\&quot;:\&quot;Beijing\&quot;,\&quot;id\&quot;:\&quot;d1001\&quot;}}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <h3>{{ t('dataIn.opentsdbjson.step32') }}</h3>
    <ul>
      <li>{{ t('dataIn.opentsdbjson.step32desc') }}</li>
      <li>{{ t('dataIn.opentsdbjson.step32desc1') }}</li>
    </ul>
    <pre
      v-highlight="
        `curl -L -d &quot;select * from ${db_name}.meter_current where groupid=2&quot; $${urlKey}/rest/sql/test?token=$${tokenKey}
`
      "
    ><code class="language-bash"></code></pre>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
import { urlKey, tokenKey } from '../utils';
import { instance, project } from 'config';

const db_name = 'test';
const cloudText = project.isCloud ? 'Cloud' : '';
</script>
