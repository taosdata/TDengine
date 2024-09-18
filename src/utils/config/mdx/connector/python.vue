<template>
  <div>
    <h2 id="install-connector">{{ $t("docs.connector.python.step1") }}</h2>
    <p>{{ $t("docs.connector.python.step1desc") }}</p>
    <el-tabs value="pip" groupID="package">
      <el-tab-pane name="pip" label="pip">
        <pre v-highlight><code>pip3 install -U taospy
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="conda" label="conda">
        <pre v-highlight><code>conda install -c conda-forge taospy
</code></pre>
      </el-tab-pane>
    </el-tabs>
    <doc-config :url="url" :token="token"></doc-config>
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
    <el-tabs value="pip" groupID="package">
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
      <a :href="`${$t('urlPart')}/reference/taos-sql/insert/`">{{
        `${$t('docs.connector.bottom2_1')}`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`${$t('urlPart')}/reference/taos-sql/select/`">{{
        `${$t('docs.connector.bottom2_2')}`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`${$t('urlPart')}${$t('docs.connector.bottom3_1')}`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script>
import DocConfig from "@/components/DocConfig/index.vue";
export default {
  components: { DocConfig },
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    }
  },
  data() {
    return {};
  },
  computed: {
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    restapi(){
      return this.$i18n.locale.includes('en') ?"reference": "connector";
    }
  },
 
};
</script>
