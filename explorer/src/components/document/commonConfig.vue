<template>
  <div>
    <h2 :id="'{{config}}'">{{ $t('docs.docConfig.title') }}</h2>
    <p>{{ $t('docs.docConfig.content', [urlDes]) }}</p>
    <p>
      <el-icon color="gold" :size="20">
        <Opportunity />
      </el-icon>
      <span class="docker-tip">{{ $t('dockerTip', [`${baseurl.split('//')[1]}`]) }}</span>
    </p>
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
    </el-tabs>
    <p>{{ $t('docs.docConfig.bottom') }}</p>
  </div>
</template>
<script setup lang="ts">
import { useRoute } from 'vue-router';
const props = withDefaults(
  defineProps<{
    url?: string;
    token?: string;
    id?: string;
    needToken?: boolean;
    urlKey?: string;
    urlDes?: string;
  }>(),
  {
    id: 'config',
    needToken: true,
    urlKey: 'TDENGINE_URL',
    urlDes: 'URL and Token',
    url: '',
    token: ''
  }
);
const route = useRoute();

const contentBash = computed(() => {
  return getContent('bash');
});
const contentCMD = computed(() => {
  return getContent('cmd');
});
const contentPower = computed(() => {
  return getContent('psh');
});
const baseurl = computed(() => {
  return localStorage.getItem('base_url') ?? '';
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
    tmpToken = `TDENGINE_TOKEN="${mtoken}"`;
  }
  switch (cType) {
    case 'bash': {
      cType = 'export ';
      break;
    }
    case 'cmd': {
      cType = 'set ';
      tmpURL = `${tmpURLKey}=${tURL}`;
      tmpToken = `TDENGINE_TOKEN=${mtoken}`;
      break;
    }
    case 'psh': {
      cType = '$env:';
      tmpURL = `${tmpURLKey}='${tURL}'`;
      tmpToken = `TDENGINE_TOKEN='${mtoken}'`;
      break;
    }
  }
  result = `${cType}${tmpURL}`;
  if (props.needToken && route.name !== 'Topic Example') {
    //数据订阅的python不展示token
    result += `\n${cType}${tmpToken}`;
  }
  return result;
}
</script>
