<template>
  <div>
    <h2 id="install-connector">{{ t('connector.python.step1') }}</h2>
    <p>{{ t('connector.python.step1desc') }}</p>
    <el-tabs model-value="pip">
      <el-tab-pane name="pip" label="Pip">
        <pre v-highlight><code>pip3 install -U taospy
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="conda" label="Conda">
        <pre v-highlight><code>conda install -c conda-forge taospy
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <doc-config
      :url-des="t('docsConfig.url')"
      :need-token="project.isCloud"
      :url="instance.gatewayUrl"
      :token="instance.token"
    ></doc-config>
    <h2 id="connect">{{ t('connector.python.step3') }}</h2>
    <p>{{ t('connector.python.step3desc') }}</p>
    <el-tabs model-value="REST">
      <el-tab-pane name="REST" label="REST">
        <pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_CLOUD_URL&quot;]
token = os.environ[&quot;TDENGINE_CLOUD_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
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

    <h2 id="jupyter">Jupyter</h2>
    <p>
      <strong>{{ t('connector.python.step41Title') }}</strong>
    </p>
    <p>{{ t('connector.python.step41Desc') }}</p>
    <el-tabs model-value="pip">
      <el-tab-pane name="pip" label="Pip">
        <pre
          v-highlight="
            `pip3 install jupyterlab
pip3 install -U taospy
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="conda" label="Conda">
        <pre v-highlight><code>conda install -c conda-forge jupyterlab
conda install -c conda-forge taospy
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <p>
      <strong>{{ t('connector.python.step42Title') }}</strong>
    </p>
    <p>{{ t('connector.python.step42Desc') }}</p>
    <pre
      v-highlight="
        `export TDENGINE_CLOUD_TOKEN=&quot;${instance.token}&quot;
export TDENGINE_CLOUD_URL=&quot;${instance.gatewayUrl}&quot;
jupyter lab
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      <strong>{{ t('connector.python.step43Title') }}</strong>
    </p>
    <p>{{ t('connector.python.step43Desc') }}</p>
    <pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_CLOUD_URL&quot;]
token = os.environ[&quot;TDENGINE_CLOUD_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
</code></pre>
    <p>
      {{ t('connector.bottom1') }} {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/cloud/programming/insert/`">{{ `${docs.urlPrefix}/cloud/programming/insert/` }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/cloud/programming/query/`">{{ `${docs.urlPrefix}/cloud/programming/query/` }}</a
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
</script>
