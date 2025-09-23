<template>
  <div>
    <h2 :id="id">{{ t('docsConfig.title') }}</h2>
    <p>{{ t('docsConfig.content', [urlDes]) }}</p>
    <el-tabs class="doc-config-tab" model-value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre v-highlight="contentBash"><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre v-highlight="contentCMD"><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre v-highlight="contentPower"><code class="language-powershell"></code></pre>
      </el-tab-pane>
      <el-tab-pane v-if="showSpringTab" name="spring" label="Spring">
        <p>{{ t('connector.java.step3confdesc') }}</p>
        <pre v-highlight="contentSpring"><code class="language-yml"></code></pre>
      </el-tab-pane>
    </el-tabs>
    <p>{{ t('docsConfig.bottom') }}</p>
  </div>
</template>
<script lang="ts" setup>
import { t } from 'locales';

interface Props {
  url: string;
  token?: string;
  id?: string;
  needToken: boolean;
  urlKey?: string;
  urlDes?: string;
  showSpringTab?: boolean;
}
const props = withDefaults(defineProps<Props>(), {
  url: '',
  token: '',
  id: 'config',
  needToken: true,
  urlKey: 'TDENGINE_CLOUD_URL',
  urlDes: 'URL and Token',
  showSpringTab: false
});
const contentBash = computed(() => getContent('bash'));
const contentCMD = computed(() => getContent('cmd'));
const contentPower = computed(() => getContent('psh'));
const contentSpring = computed(() => {
  return `server:
  port: 8080

spring:
  datasource:
    driver-class-name: com.taosdata.jdbc.ws.WebSocketDriver
    url: ${props.url}
# using connection pools
    type: com.alibaba.druid.pool.DruidDataSource
    druid:
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000
      time-between-eviction-runs-millis: 60000
      min-evictable-idle-time-millis: 300000
# mybatis
mybatis:
  mapper-locations: classpath:mapper/*.xml`;
});

function getContent(cType: string) {
  let result = '';
  const tmpURLKey = props.urlKey;
  const tURL = props.url;
  let tmpURL = `${tmpURLKey}="${tURL}"`;
  let tmpToken = '';
  let mtoken = '';
  if (props.needToken) {
    mtoken = props.token;
    tmpToken = `TDENGINE_CLOUD_TOKEN="${mtoken}"`;
  }
  switch (cType) {
    case 'bash': {
      cType = 'export ';
      break;
    }
    case 'cmd': {
      cType = 'set ';
      tmpURL = `${tmpURLKey}=${tURL}`;
      tmpToken = `TDENGINE_CLOUD_TOKEN=${mtoken}`;
      break;
    }
    case 'psh': {
      cType = '$env:';
      tmpURL = `${tmpURLKey}='${tURL}'`;
      tmpToken = `TDENGINE_CLOUD_TOKEN='${mtoken}'`;
      break;
    }
  }
  result = `${cType}${tmpURL}`;
  if (props.needToken) {
    result += `\n${cType}${tmpToken}`;
  }
  return result;
}
</script>
