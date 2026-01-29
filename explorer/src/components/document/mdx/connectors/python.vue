<template>
  <div>
    <h2 id="install-connector">{{ $t("docs.connector.python.step1") }}</h2>
    <p>{{ $t("docs.connector.python.step1desc") }}</p>
    <el-tabs model-value="pip" group-i-d="package">
      <el-tab-pane name="pip" label="pip">
        <pre v-highlight><code>pip3 install -U taospy
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="conda" label="conda">
        <pre v-highlight><code>conda install -c conda-forge taospy
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <DocConfig :url="url" :token="token"></DocConfig>
    <h2 id="connect">{{ $t("docs.connector.python.step3") }}</h2>
    <p>{{ $t("docs.connector.python.step3desc") }}</p>
    <pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_URL&quot;]
token = os.environ[&quot;TDENGINE_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
</code></pre>
    <h2 id="jupyter">Jupyter</h2>
    <p>
      <strong>{{ $t("docs.connector.python.step41Title") }}</strong>
    </p>
    <p>{{ $t("docs.connector.python.step41Desc") }}</p>
    <el-tabs model-value="pip" group-i-d="package">
      <el-tab-pane name="pip" label="pip">
        <pre
          v-highlight="
            `pip install jupyterlab
pip3 install -U taospy
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="conda" label="conda">
        <pre v-highlight><code>conda install -c conda-forge jupyterlab
conda install -c conda-forge taospy
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <p>
      <strong>{{ $t("docs.connector.python.step42Title") }}</strong>
    </p>
    <p>{{ $t("docs.connector.python.step42Desc") }}</p>
    <pre
      v-highlight="
        `export TDENGINE_TOKEN=&quot;${token}&quot;
export TDENGINE_URL=&quot;${url}&quot;
jupyter lab
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      <strong>{{ $t("docs.connector.python.step43Title") }}</strong>
    </p>
    <p>{{ $t("docs.connector.python.step43Desc") }}</p>
    <pre v-highlight><code class="language-python">import taosrest
import os

url = os.environ[&quot;TDENGINE_URL&quot;]
token = os.environ[&quot;TDENGINE_TOKEN&quot;]

conn = taosrest.connect(url=url, token=token)
</code></pre>
     <p>
      {{ $t("docs.connector.bottom1") }} {{ $t("docs.connector.bottom2") }}
      <a :href="`${$t('urlPart')}/${insertApi}`">{{
        `${$t('docs.connector.bottom2_1')}`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`${$t('urlPart')}/${selectApi}`">{{
        `${$t('docs.connector.bottom2_2')}`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`${$t('urlPart')}/${restApi}`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script setup lang="ts">
import DocConfig from "@/components/document/commonConfig.vue";
import { DocsProps } from '../utils'
import { isEn } from "@/const";

defineProps<DocsProps>()

const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
const insertApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#insert-data' : 'develop/sql/#插入数据');
const selectApi = computed(() => isEn.value ? 'developer-guide/running-sql-statements/#query-data' : 'develop/sql/#查询数据');

</script>
