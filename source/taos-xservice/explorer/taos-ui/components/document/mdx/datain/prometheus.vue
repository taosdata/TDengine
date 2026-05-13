<template>
  <div> 
    <p>{{ t('dataIn.prometheus.totaldesc1') }}</p>
    <p>{{ t('dataIn.prometheus.totaldesc2') }}</p>
    <h2 id="prerequisites">{{ t('dataIn.prometheus.step1') }}</h2>
    <p>{{ t('dataIn.prometheus.step1desc', [cloudText]) }}</p>
    <h2 id="install-prometheus">{{ t('dataIn.prometheus.step2') }}</h2>
    <p>{{ t('dataIn.prometheus.step2desc') }}</p>
    <ol>
      <li>
        {{ t('dataIn.prometheus.step21') }}
        <pre
          v-highlight
        ><code>wget https://github.com/prometheus/prometheus/releases/download/v2.37.0/prometheus-2.37.0.linux-amd64.tar.gz
</code></pre>
      </li>
      <li>
        {{ t('dataIn.prometheus.step22') }}
        <pre v-highlight><code>tar xvfz prometheus-*.tar.gz &amp;&amp; mv prometheus-2.37.0.linux-amd64 prometheus
</code></pre>
      </li>
      <li>
        {{ t('dataIn.prometheus.step23') }}
        <pre v-highlight><code>cd prometheus
</code></pre>
      </li>
    </ol>
    <p>
      {{ t('dataIn.prometheus.step2end') }}
      <a href="https://prometheus.io/docs/prometheus/latest/installation/">{{ t('dataIn.prometheus.step2doc') }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <h2 id="configure-prometheus">{{ t('dataIn.prometheus.step3') }}</h2>
    <p>{{ t('dataIn.prometheus.step3desc') }}</p>
    <pre
      v-highlight="
        `remote_write:
  - url: &quot;${instance.gatewayUrl}/prometheus/v1/remote_write/prometheus_data?token=${instance.token}&quot;

remote_read:
  - url: &quot;${instance.gatewayUrl}/prometheus/v1/remote_read/prometheus_data?token=${instance.token}&quot;
    remote_timeout: 10s
    read_recent: true
`
      "
    ><code class="language-yaml"></code></pre>
    <p>{{ t('dataIn.prometheus.step3desc1') }}</p>
    <h2 id="start-prometheus">{{ t('dataIn.prometheus.step4') }}</h2>
    <pre v-highlight><code>./prometheus --config.file prometheus.yml
</code></pre>
    <p>
      {{ t('dataIn.prometheus.step4desc') }}<a href="http://localhost:9090">http://localhost:9090</a
      >{{ t('dataIn.prometheus.step4desc1') }}
    </p>
    <h2 id="verify-remote-write">{{ t('dataIn.prometheus.step5') }}</h2>
    <p>{{ t('dataIn.prometheus.step5desc') }}</p>
    <p>
      <img src="../assets/prometheus/prometheus_data.webp" alt="TDengine prometheus remote_write result" />
    </p>
    <ul>
      <li>{{ t('dataIn.prometheus.step5desc1') }}</li>
    </ul>
  </div>
</template>

<script lang="ts" setup>
import { instance, project } from 'config';
import { t } from 'locales';
const cloudText = project.isCloud ? 'Cloud' : '';
</script>
