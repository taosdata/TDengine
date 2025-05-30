<template>
  <div>
    <p>
      {{ $t('docs.virtual.grafana.topdesc') }}<a href="https://www.grafana.com/"> Grafana</a
    >{{
        $t('docs.virtual.grafana.topdesc1')
      }}<a href="https://github.com/taosdata/grafanaplugin/blob/master/README.md">
      GitHub</a>{{ $t('docs.virtual.grafana.topdesc2') }}
    </p>
    <h2 id="install-grafana">{{ $t('docs.virtual.grafana.step1') }}</h2>
    <p>
      {{ $t('docs.virtual.grafana.step1desc') }}<a href="https://grafana.com/grafana/download"
    >https://grafana.com/grafana/download</a
    >{{ $t('docs.virtual.grafana.step1desc1') }}
    </p>
    <h2 id="install-tdengine-plugin">{{ $t('docs.virtual.grafana.step2') }}</h2>
    <el-tabs v-model="activeTab">
      <el-tab-pane name="Grafana Cli" label="Grafana Cli">
        <p v-dompurify-html="$t('docs.virtual.grafana.step2desc')"></p>
        <pre
          v-highlight="`sudo -u grafana grafana-cli --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v3.7.2/tdengine-datasource-3.7.2.zip plugins install tdengine-datasource
`
          "
        ><code
          class="language-bash"></code></pre>
        <p v-dompurify-html="$t('docs.virtual.grafana.step2desc1')"></p>
        <pre
          v-highlight="
            `./grafana-cli.exe --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v3.7.2/tdengine-datasource-3.7.2.zip plugins install tdengine-datasource
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="script" label="Script"
      ><p>{{ $t('docs.virtual.grafana.script1') }}</p>
        <pre
          v-highlight="
            `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ $t('docs.virtual.grafana.script2') }}</p>
        <pre
          v-highlight="
            `sudo systemctl restart grafana-server.service
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>
    <h2 id="verify-plugin">{{ $t('docs.virtual.grafana.step3') }}</h2>
    <p>{{ $t('docs.virtual.grafana.step3desc') }}</p>
    <p>
      <el-icon color="gold" :size="20">
        <Opportunity/>
      </el-icon>
      <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
    </p>
    <div style="display: flex; align-items: baseline; margin-bottom: 0">
      <span style="width: 100px">{{ $t('docs.virtual.grafana.step3desc1') }}</span>
      <pre
        v-highlight="
          `${url}
`
        "
      ><code class="language-bash"></code></pre>
    </div>
    <div style="display: flex; align-items: baseline; margin-bottom: 0">
      <span style="width: 100px">{{ $t('docs.virtual.grafana.step3desc2') }}</span>
      <pre
        v-highlight="
          `${user}
`
        "
      ><code class="language-bash"></code></pre>
    </div>

    <p v-dompurify-htmlurify-html="$t('docs.virtual.grafana.step3desc3')"></p>
    <h2 id="use-grafana">{{ $t('docs.virtual.grafana.step4') }}</h2>
    <p>
      {{ $t('docs.virtual.grafana.step4desc') }}
      <span v-if="!$IS_OEM">
        {{ $t('docs.virtual.grafana.step4desc1') }}
        <a :href="`${$t('urlPart')}/third-party/grafana#create-dashboard`">{{
            $t('docs.virtual.grafana.step4desc2')
          }}</a>
        {{ $t('docs.virtual.grafana.step4desc3') }}</span
      >
    </p>
  </div>
</template>

<script setup lang="ts">
import {DocsProps} from '../utils';

defineProps<DocsProps>();
const {$IS_OEM} = inject('globalCustomProperties') as GlobalCustomProperties;

const activeTab = ref('Grafana Cli');
</script>
<style>
.pre-code {
  width: 100%;
}
</style>
