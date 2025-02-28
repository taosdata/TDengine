<template>
  <div>
    <p>
      {{ t('dataIn.influxdb.desc', [t('dataIn.influxdb.title'), cloudText]) }}
    </p>
    <h2 id="config">{{ t('dataIn.influxdb.step1') }}</h2>
    <p>{{ t('dataIn.influxdb.step1desc', [cloudText]) }}</p>
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

    <h2 id="insert">{{ t('dataIn.influxdb.step2') }}</h2>
    <p>{{ t('dataIn.influxdb.step2desc') }}</p>
    <pre v-highlight><code class="language-text">/influxdb/v1/write?db=&lt;db_name&gt;&amp;token=&lt;cloud_token&gt;
</code></pre>
    <p>{{ t('dataIn.influxdb.step2desc1') }}</p>
    <ul>
      <li>{{ t('dataIn.influxdb.step2desc2') }}</li>
      <li>
        {{ t('dataIn.influxdb.step2desc3') }}
        <ul>
          <li>ns - {{ t('dataIn.influxdb.step2desc3ns') }}</li>
          <li>u - {{ t('dataIn.influxdb.step2desc3u') }}</li>
          <li>ms - {{ t('dataIn.influxdb.step2desc3ms') }}</li>
          <li>s - {{ t('dataIn.influxdb.step2desc3s') }}</li>
          <li>m - {{ t('dataIn.influxdb.step2desc3m') }}</li>
          <li>h - {{ t('dataIn.influxdb.step2desc3h') }}</li>
        </ul>
      </li>
    </ul>
    <h2 id="examples">{{ t('dataIn.influxdb.step3') }}</h2>
    <h3>{{ t('dataIn.influxdb.step31') }}</h3>
    <pre
      v-highlight="
        `curl --request POST &quot;$${urlKey}/influxdb/v1/write?db=${db_name}&amp;token=$${tokenKey}&amp;precision=ns&quot; --data-binary &quot;measurement,host=host1 field1=2i,field2=2.0 1577846800001000001&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <h3>{{ t('dataIn.influxdb.step32') }}</h3>
    <ul>
      <li>{{ t('dataIn.influxdb.step32desc') }}</li>
      <li>{{ t('dataIn.influxdb.step32desc1') }}</li>
    </ul>
    <pre
      v-highlight="
        `curl -L -d &quot;select * from ${db_name}.measurement where host=\&quot;host1\&quot;&quot; $${urlKey}/rest/sql/test?token=$${tokenKey}
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
