<template>
  <div>
    <h2 id="install-connector">{{ $t("docs.connector.node.step1") }}</h2>
    <pre
      v-highlight="
        `npm install @tdengine/rest
`
      "
    ><code class="language-bash"></code></pre>
    <doc-config :url="url" :token="token"></doc-config>
    <h2 id="connect">{{ $t("docs.connector.node.step3") }}</h2>
    <pre
      v-highlight
    ><code class="language-javascript">const { options, connect } = require(&quot;@tdengine/rest&quot;);

async function test() {
  options.url = process.env.TDENGINE_URL;
  options.query = { token: process.env.TDENGINE_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query(&quot;show databases&quot;);
    res.toString();
  } catch (err) {
    console.log(err);
  }
}

test();
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
import { IsAliyun } from "@/const";
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
    },
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
