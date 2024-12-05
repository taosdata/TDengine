<template>
  <div>
    <h2 id="install-connector">{{ $t("docs.connector.node.step1") }}</h2>
    <pre
      v-highlight="'npm install @tdengine/websocket'"
    ><code class="language-bash"></code></pre>
    <doc-config :url="url" :need-token="false"></doc-config>
    <h2 id="connect">{{ $t("docs.connector.node.step3") }}</h2>
    <pre
      v-highlight
    ><code class="language-javascript">const taos = require("@tdengine/websocket");
const dsn = process.env.TDENGINE_URL;

async function test() {
    let conn = null;
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conn = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
  } catch (err) {
    console.log("Failed to connect to " + dsn + ", ErrCode: " + err.code + ", ErrMessage: " + err.message);
    return;
  }

  let wsRows = null;
    try {
        wsRows = await conn.query("show databases");
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('database: ' + row[0] );
        }
    console.log("successfully!")
    } catch (err) {
        console.error(`Failed to query data from power.meters, sql: ${sql}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        return;
    } finally {
        if (wsRows) {
            await wsRows.close();
        }
    if (conn) {
            await conn.close();
        }
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
