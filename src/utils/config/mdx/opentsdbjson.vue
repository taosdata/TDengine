<template>
  <div>
    <p>
      {{
        $t("docs.party.influxdb.desc", [$t("docs.party.opentsdbjson.title")])
      }}
    </p>
    <h2 id="config">{{ $t("docs.party.opentsdbjson.step1") }}</h2>
    <p>
      {{
        $t("component.docConfig.content", [
          " Token " + $t("docs.connector.bottomand") + " URL ",
        ])
      }}
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

    <h2 id="insert">{{ $t("docs.party.opentsdbjson.step2") }}</h2>
    <p>{{ $t("docs.party.opentsdbjson.step2desc") }}</p>
    <pre
      v-highlight
    ><code class="language-text">/opentsdb/v1/put/json/&lt;db&gt;?token=&lt;cloud_token&gt;
</code></pre>
    <h2 id="examples">{{ $t("docs.party.opentsdbjson.step3") }}</h2>
    <h3>{{ $t("docs.party.opentsdbjson.step31") }}</h3>
    <pre
      v-highlight="
        `curl --request POST &quot;$TDENGINE_URL/opentsdb/v1/put/json/${db_name}?token=$TDENGINE_TOKEN&quot; --data-binary &quot;{\&quot;metric\&quot;:\&quot;meter_current\&quot;,\&quot;timestamp\&quot;:1646846400,\&quot;value\&quot;:10.3,\&quot;tags\&quot;:{\&quot;groupid\&quot;:2,\&quot;location\&quot;:\&quot;Beijing\&quot;,\&quot;id\&quot;:\&quot;d1001\&quot;}}&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <h3>{{ $t("docs.party.opentsdbjson.step32") }}</h3>
    <ul>
      <li>{{ $t("docs.party.opentsdbjson.step32desc") }}</li>
      <li>{{ $t("docs.party.opentsdbjson.step32desc1") }}</li>
    </ul>
    <pre
      v-highlight="
        `curl -L -d &quot;select * from ${db_name}.meter_current where groupid=2&quot; $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`
      "
    ><code class="language-bash"></code></pre>
  </div>
</template>

<script>
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
    return {
      db_name: "test",
    };
  },
  computed: {
    jdbcURL() {
      return (
        "jdbc:TAOS-RS://" +
        this.url.replace(/https?:\/\//, "") +
        "?usessl=" +
        this.url.startsWith("https") +
        "&token=" +
        this.token
      );
    },
    goDSN() {
      return (
        (this.url.startsWith("https") ? "https" : "http") +
        "(" +
        this.url.replace(/https?:\/\//, "") +
        ")/?token=" +
        this.token
      );
    },
    DSN() {
      return this.url + "?token=" + this.token;
    },
    cloud_url() {
      return this.url;
    },
    cloud_token() {
      return this.token;
    },
  },
};
</script>
