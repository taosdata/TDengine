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
      <a :href="`https://docs.${urlPart}.com/develop/insert-data/`">{{
        `https://docs.${urlPart}.com/develop/insert-data/`
      }}</a>
      {{ $t("docs.connector.bottomand") }}
      <a :href="`https://docs.${urlPart}.com/develop/query-data/`">{{
        `https://docs.${urlPart}.com/develop/query-data/`
      }}</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`https://docs.${urlPart}.com/reference/rest-api/`"
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
      return IsAliyun ? "taosdata" : "tdengine";
    },
  },
};
</script>
