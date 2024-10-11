<template>
  <div>
    <p>{{ $t("docs.connector.rest.desc") }}</p>
    <h2 id="config">{{ $t("docs.connector.rest.step1") }}</h2>
    <p>
      {{
        $t("component.docConfig.content", [
          " Token " + $t("docs.connector.bottomand") + " URL ",
        ])
      }}
      <span class="docker-tip">{{ $t("dockerTip")}}</span>
    </p>
    <p>
      <i class="el-icon-s-opportunity" style="color: gold;font-size: 20px"></i>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <el-tabs value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_TOKEN=&quot;${token}&quot;
export TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_TOKEN=&quot;${token}&quot;
set TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_TOKEN=&quot;${token}&quot;
$env:TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="insert">{{ $t("docs.connector.rest.step2") }}</h2>
    <p>{{ $t("docs.connector.rest.step2desc") }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31)&quot; \
  $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`
      "
    ><code class="language-bash"></code></pre>

    <h2 id="query">{{ $t("docs.connector.rest.step3") }}</h2>
    <p>{{ $t("docs.connector.rest.step3desc") }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;select name, ntables, status from information_schema.ins_databases;&quot; \
  $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`
      "
    ><code class="language-bash"></code></pre>
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
import { IsAliyun } from "@/const";
export default {
  props: {
    token: {
      type: String,
      default: "",
    },
    url: {
      type: String,
      default: "",
    },
  },
  data() {
    return {};
  },
  computed: {
    DSN() {
      return this.url + "?token=" + this.token;
    },
    urlPart() {
      return this.$i18n.locale.includes('en') ?"tdengine": "taosdata";
    },
    restapi(){
      return this.$i18n.locale.includes('en') ?"reference": "connector";
    }
  },
};
</script>
