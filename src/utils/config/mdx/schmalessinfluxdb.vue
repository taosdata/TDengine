<template>
  <div>
    <p>
      {{ $t("docs.party.influxdb.desc", [$t("docs.party.influxdb.title")]) }}
    </p>
    <h2 id="config">{{ $t("docs.party.influxdb.step1") }}</h2>
    <p>{{ $t("docs.party.influxdb.step1desc") }}</p>
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

    <h2 id="insert">{{ $t("docs.party.influxdb.step2") }}</h2>
    <p>{{ $t("docs.party.influxdb.step2desc") }}</p>
    <pre
      v-highlight
    ><code class="language-text">/influxdb/v1/write?db=&lt;db_name&gt;&amp;token=&lt;cloud_token&gt;
</code></pre>
    <p>{{ $t("docs.party.influxdb.step2desc1") }}</p>
    <ul>
      <li>{{ $t("docs.party.influxdb.step2desc2") }}</li>
      <li>
        {{ $t("docs.party.influxdb.step2desc3") }}
        <ul>
          <li>ns - {{ $t("docs.party.influxdb.step2desc3ns") }}</li>
          <li>u - {{ $t("docs.party.influxdb.step2desc3u") }}</li>
          <li>ms - {{ $t("docs.party.influxdb.step2desc3ms") }}</li>
          <li>s - {{ $t("docs.party.influxdb.step2desc3s") }}</li>
          <li>m - {{ $t("docs.party.influxdb.step2desc3m") }}</li>
          <li>h - {{ $t("docs.party.influxdb.step2desc3h") }}</li>
        </ul>
      </li>
    </ul>
    <h2 id="examples">{{ $t("docs.party.influxdb.step3") }}</h2>
    <h3>{{ $t("docs.party.influxdb.step31") }}</h3>
    <pre
      v-highlight="
        `curl --request POST &quot;$TDENGINE_URL/influxdb/v1/write?db=<db_name>&amp;token=$TDENGINE_TOKEN&amp;precision=ns&quot; --data-binary &quot;measurement,host=host1 field1=2i,field2=2.0 1577846800001000001&quot;
`
      "
    ><code class="language-bash"></code></pre>
    <h3>{{ $t("docs.party.influxdb.step32") }}</h3>
    <ul>
      <li>{{ $t("docs.party.influxdb.step32desc") }}</li>
      <li>{{ $t("docs.party.influxdb.step32desc1") }}</li>
    </ul>
    <pre
      v-highlight="
        `curl -L -d &quot;select * from <db_name>.measurement where host=\&quot;host1\&quot;&quot; $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
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
    return {};
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
