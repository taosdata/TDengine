<template>
  <div>
    <p>
      {{ $t('docs.dashboard.topdesc') }}<a href="https://www.grafana.com/"> Grafana</a
      >{{ $t('docs.dashboard.topdesc1') }}
      <!-- <a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >. -->
    </p>
    <!-- <p>
      {{ $t("docs.virtual.grafana.topdesc2")
      }}<a
        href="https://github.com/taosdata/grafanaplugin/blob/master/README.md"
        >GitHub</a
      >.
    </p> -->

    <h2 id="install-grafana">{{ $t('docs.virtual.grafana.step1') }}</h2>
    <p>
      {{ $t('docs.virtual.grafana.step1desc') }}(<a href="https://grafana.com/grafana/download"
        >https://grafana.com/grafana/download</a
      >).
    </p>
    <h2 id="install-tdengine-plugin">{{ $t('docs.virtual.grafana.step2') }}</h2>
    <el-tabs v-model="activeTab">
      <el-tab-pane name="plugins" label="Plugins">
        <!-- <p>{{$t('docs.virtual.grafana.pluginsdesc')}}</p> -->
        <p v-dompurify-html="$t('docs.virtual.grafana.plugin1desc')"></p>
        <p v-dompurify-html="$t('docs.virtual.grafana.plugin2desc')"></p>
        <p v-dompurify-html="$t('docs.virtual.grafana.plugin3desc')"></p>
        <p v-dompurify-html="$t('docs.virtual.grafana.plugin4desc')"></p>
      </el-tab-pane>
      <el-tab-pane name="script" label="Script"
        ><p>{{ $t('docs.virtual.grafana.script1') }}</p>
        <pre
          v-highlight="
            `bash -c &quot;$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)&quot;
`
          "
        ><code class="language-bash"></code></pre>
        <p v-dompurify-html="$t('docs.virtual.grafana.script2')"></p>
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
        <Opportunity />
      </el-icon>
      <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
    </p>
    <div style="display: flex; align-items: baseline; margin-bottom: 0">
      <span style="width: 60px">{{ $t('docs.virtual.grafana.step3desc1') }}</span>
      <pre v-highlight="`${url}`"><code class="language-bash"></code></pre>
    </div>
    <div style="display: flex; align-items: baseline; margin-bottom: 0">
      <span style="width: 60px">{{ $t('docs.virtual.grafana.step3desc2') }}</span>
      <pre v-highlight="`${user}`"><code class="language-bash"></code></pre>
    </div>

    <p v-dompurify-html="$t('docs.virtual.grafana.step3desc3')"></p>

    <h2 id="import-dashboard">{{ $t('docs.dashboard.step5') }}</h2>
    <p v-dompurify-html="$t('docs.dashboard.desc51')"></p>
    <p v-dompurify-html="$t('docs.dashboard.desc52')"></p>
    <p v-dompurify-html="$t('docs.dashboard.desc53')"></p>
    <p v-dompurify-html="$t('docs.dashboard.desc54')"></p>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from './utils';

defineProps<DocsProps>();
const activeTab = ref('plugins');
</script>
<style lang="scss" scoped>
img {
  width: 894px;

  // height: 229px;
}

.copy-icon,
.copy-icon2,
.copy-icon3,
.copy-icon4,
.copy-icon5,
.copy-icon6 {
  position: absolute;
  top: 20px;
  right: 20px;
  display: flex;
  align-items: center;
  color: #4259ce;
  white-space: nowrap;
  cursor: pointer;
  visibility: hidden;
}

.first,
.second,
.fifth,
.fourth,
.sixth {
  padding-bottom: 5px;
  line-height: 0.5 !important;
}

.first:hover {
  .copy-icon {
    visibility: visible;
  }
}

.second:hover {
  .copy-icon2 {
    visibility: visible;
  }
}

.third:hover {
  .copy-icon3 {
    visibility: visible;
  }
}

.fourth:hover {
  .copy-icon4 {
    visibility: visible;
  }
}

.fifth:hover {
  .copy-icon5 {
    visibility: visible;
  }
}

.sixth:hover {
  .copy-icon6 {
    visibility: visible;
  }
}

.float {
  position: absolute;
  top: -80px;
  right: -30px;
  z-index: 999;
  box-sizing: border-box;
  display: flex;
  width: 330px;
  padding: 14px 26px 14px 13px;
  overflow: hidden;
  background-color: #fff;
  border: 1px solid #ebeef5;
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgb(0 0 0 / 10%);
  transition:
    opacity 0.3s,
    transform 0.3s,
    left 0.3s,
    right 0.3s,
    top 0.4s,
    bottom 0.3s;
}
</style>
<style lang="scss">
::v-deep {
  .markdown-body pre {
    line-height: 0.1 !important;
  }
}
</style>
