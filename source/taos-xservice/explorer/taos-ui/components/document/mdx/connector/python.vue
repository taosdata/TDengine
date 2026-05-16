<template>
  <div>
    <h2 id="install-connector">{{ t('connector.python.step1') }}</h2>
    <div v-dompurify-html="t('connector.python.step1-1')"></div>
    <div v-dompurify-html="t('connector.python.step1-2')"></div>
    <el-tabs v-model="activeName" group-i-d="package">
      <el-tab-pane name="REST" label="REST">
        <pre v-highlight><code>pip3 uninstall taos taospy
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="WebSocket" label="WebSocket">
        <pre v-highlight><code>pip3 uninstall taos taos-ws-py
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <div v-dompurify-html="t('connector.python.step1-2-1')"></div>
    <el-tabs v-model="activeName" group-i-d="package">
      <el-tab-pane name="REST" label="REST">
        <pre v-highlight><code># install latest version
pip3 install taospy

# install specific version
pip3 install taospy==2.6.2

# install from github
pip3 install git+https://github.com/taosdata/taos-connector-python.git
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="WebSocket" label="WebSocket">
        <pre v-highlight><code>pip3 install taos-ws-py
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <h3>{{ t('connector.python.step1-3') }}</h3>
    <el-tabs v-model="activeName" group-i-d="package">
      <el-tab-pane name="REST" label="REST">
        <p v-dompurify-html="$t('connector.python.step1-3-1')"></p>
        <pre v-highlight><code>import taosrest
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="WebSocket" label="WebSocket">
        <p v-dompurify-html="$t('connector.python.step1-3-2')"></p>
        <pre v-highlight><code>import taosws
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <doc-config
      :url-des="t('docsConfig.url')"
      :need-token="project.isCloud"
      :url="dsn"
      :token="instance.token"
    ></doc-config>
    <h2 id="connect">{{ t('connector.python.step3') }}</h2>
    <p>{{ t('connector.python.step3desc') }}</p>
    <el-tabs v-model="activeName">
      <el-tab-pane name="REST" label="REST">
        <pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

try:
    conn = taosrest.connect(url=url, token=token)
    # test the connection by getting version info
    print("TDengine version: ", conn.server_info)
except Exception as e:
    print(str(e))
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="WebSocket" label="WebSocket">
        <pre v-highlight><code class="language-python">import taosws
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

try:
    conn = taosws.connect("%s?token=%s" % (url, token))
except Exception as e:
    print(str(e))</code></pre>
      </el-tab-pane>
    </el-tabs>

    <p>
      {{ t('connector.bottom1') }} {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/cloud/programming/insert/`">{{ t('common.insert') }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/cloud/programming/query/`">{{ t('common.query') }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/cloud/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { docs, project, instance } from 'config';

const activeName = ref('REST');
const dsn = computed(() => {
  return activeName.value === 'REST' ? instance.gatewayUrl : instance.gatewayUrl.replace('http', 'ws');
});
</script>
