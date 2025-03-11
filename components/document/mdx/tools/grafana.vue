<template>
  <div>
    <p>
      {{ t('tools.grafana.topdesc') }}<a href="https://www.grafana.com/">Grafana</a>{{ t('tools.grafana.topdesc1') }}
      <a href="https://github.com/taosdata/grafanaplugin/blob/master/README.md">GitHub</a
      >{{ t('tools.grafana.topdesc3') }}
    </p>
    <h2 id="install-grafana">{{ t('tools.grafana.step1') }}</h2>
    <p>
      {{ t('tools.grafana.step1desc')
      }}<a href="https://grafana.com/grafana/download">https://grafana.com/grafana/download</a
      >{{ t('tools.grafana.step1desc1') }}
    </p>
    <h2 id="install-tdengine-plugin">{{ t('tools.grafana.step2') }}</h2>
    <el-tabs model-value="plugins">
      <el-tab-pane name="plugins" label="Plugins">
        <ol class="seeq-ol">
          <li>
            <span class="seeq-span">{{ t('tools.grafana.step2desc') }}</span>
          </li>
          <li>
            <span class="seeq-span">{{ t('tools.grafana.step2desc11') }}</span>
          </li>
          <li>
            <span class="seeq-span">{{ t('tools.grafana.step2desc12') }}</span>
          </li>
          <li>
            <span class="seeq-span">{{ t('tools.grafana.step2desc13') }}</span>
          </li>
        </ol>
      </el-tab-pane>
      <el-tab-pane name="script" label="Script">
        <p>{{ t('tools.grafana.step2desc2') }}</p>
        <pre
          v-highlight="
            `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ t('tools.grafana.step2desc3') }}</p>
        <pre
          v-highlight="
            `sudo systemctl restart grafana-server.service
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="verify-plugin">{{ t('tools.grafana.step3') }}</h2>
    <p>{{ t('tools.grafana.step3desc') }}</p>
    <div class="gf-input">
      <div class="gf-input-label">{{ t('tools.grafana.step3desc1') }}</div>
      <CopyText :text="instance.gatewayUrl" class="gf-input-value" />
    </div>
    <div class="gf-input">
      <div class="gf-input-label">{{ t('tools.grafana.step3desc2') }}</div>
      <CopyText :text="instance.token" class="gf-input-value" />
    </div>
    <p>{{ t('tools.grafana.step3desc3') }}</p>
    <h2 id="use-grafana">{{ t('tools.grafana.step4') }}</h2>
    <p>
      {{ t('tools.grafana.step4desc')
      }}<a :href="`${TdDocsUrl}/third-party/grafana#create-dashboard`">{{ t('tools.grafana.step4desc2') }}</a
      >{{ t('tools.grafana.step4desc3') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
import { TdDocsUrl, instance } from 'config';
</script>
<style scoped lang="scss">
.gf-input {
  display: flex;
  align-items: center;
  margin-bottom: 5px;

  .gf-input-label {
    min-width: 70px;
  }

  .gf-input-value {
    min-width: 500px;
    padding: 0 10px;
    background-color: #f6f8fa;
    border-radius: 5px;
  }
}

.seeq-ol {
  padding-left: 0;

  .seeq-span {
    line-height: 30px;
  }
}
</style>
