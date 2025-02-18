<template>
  <div>
    <p>{{ t('dataIn.telegraf.totaldesc1') }}</p>
    <p>{{ t('dataIn.telegraf.totaldesc2') }}</p>
    <h2 id="prerequisites">{{ t('dataIn.telegraf.step1') }}</h2>
    <p>{{ t('dataIn.telegraf.step1desc', [cloudText]) }}</p>
    <h2 id="install-telegraf">{{ t('dataIn.telegraf.step2') }}</h2>
    <p>{{ t('dataIn.telegraf.step2desc') }}</p>
    <pre
      v-highlight="
        `wget -q https://repos.influxdata.com/influxdb.key
echo &#39;23a1c8836f0afc5ed24e0486339d7cc8f6790b83886c4c96995b88a061c5bb5d influxdb.key&#39; | sha256sum -c &amp;&amp; cat influxdb.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdb.gpg } /dev/null
echo &#39;deb [signed-by=/etc/apt/trusted.gpg.d/influxdb.gpg] https://repos.influxdata.com/debian stable main&#39; | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo apt-get update &amp;&amp; sudo apt-get install telegraf
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('dataIn.telegraf.step2desc1') }}</p>
    <pre
      v-highlight="
        `sudo systemctl stop telegraf
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      {{ t('dataIn.telegraf.step2end')
      }}<a href="https://docs.influxdata.com/telegraf/v1/install/"> {{ t('dataIn.telegraf.step2doc') }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <h2 id="configure">{{ t('dataIn.telegraf.step3') }}</h2>
    <p>{{ t('dataIn.telegraf.step3desc', [cloudText]) }}</p>
    <pre
      v-highlight="
        `export ${urlKey}=&quot;${instance.gatewayUrl}&quot;
export TDENGINE_CLOUD_TOKEN=&quot;${instance.token}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('dataIn.telegraf.step3desc1') }}</p>
    <pre
      v-highlight="
        `telegraf --sample-config --input-filter cpu:mem --output-filter http > telegraf.conf
`
      "
    ><code class="language-bash"></code></pre>
    <p>{{ t('dataIn.telegraf.step3desc2') }}</p>
    <pre v-highlight><code class="language-toml">[[outputs.http]]
  url = &quot;$&#123;{{ urlKey }}&#125;/influxdb/v1/write?db=telegraf&amp;token=$&#123;{{ tokenKey }}&#125;&quot;
  method = &quot;POST&quot;
  timeout = &quot;5s&quot;
  data_format = &quot;influx&quot;
  influx_max_line_bytes = 250
</code></pre>
    <p>{{ t('dataIn.telegraf.step3desc3') }}</p>
    <h2 id="start-telegraf">{{ t('dataIn.telegraf.step4') }}</h2>
    <p>{{ t('dataIn.telegraf.step4desc') }}</p>
    <pre
      v-highlight="
        `telegraf --config telegraf.conf
`
      "
    ><code class="language-bash"></code></pre>
    <h2 id="verify">{{ t('dataIn.telegraf.step5') }}</h2>
    <p>{{ t('dataIn.telegraf.step5desc') }}</p>
    <pre v-highlight><code class="language-sql">show databases;
</code></pre>
    <p>
      <img src="../assets/telegraf/telegraf-show-databases.webp" alt="TDengine show telegraf databases" />
    </p>
    <p>{{ t('dataIn.telegraf.step5desc1') }}</p>
    <pre v-highlight><code class="language-sql">show telegraf.stables;
</code></pre>
    <p>
      <img src="../assets/telegraf/telegraf-show-stables.webp" alt="TDengine Cloud show telegraf stables" />
    </p>
    <p>
      {{ t('dataIn.telegraf.step5desc2') }}
      <a href="https://docs.influxdata.com/telegraf/v1.22/plugins/"> {{ t('dataIn.telegraf.step5desc2input') }}</a>
      {{ t('dataIn.telegraf.step5desc2insert') }}
      <a href="https://docs.influxdata.com/telegraf/v1.24/data_formats/input/">
        {{ t('dataIn.telegraf.step5desc2format') }}</a
      >
      {{ t('dataIn.telegraf.step5desc2end') }}
    </p>
    <p>
      {{ t('dataIn.telegraf.step5desc3') }}
      <a :href="TdDocsUrl + '/develop/schemaless/'">{{ t('dataIn.telegraf.step5desc3end') }}</a>
    </p>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
import { urlKey, tokenKey } from '../utils';
import { TdDocsUrl, instance, project } from 'config';
const cloudText = project.isCloud ? 'Cloud' : '';
</script>
